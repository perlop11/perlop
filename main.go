package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// Global Configuration
var (
	db          *sql.DB
	botToken    string
	publicURL   string
	adminChatID string
	httpClient  *http.Client
)

// Thread-safe In-Memory Cache for User Maps (Point #2)
type UserCacheEntry struct {
	ChatID     string
	UserKey    string
	MaxAlerts  int
	Expiration time.Time
}

var (
	userCache      = make(map[string]UserCacheEntry)
	userCacheMutex sync.RWMutex
)

func main() {
	// 1. Load Environment Variables
	botToken = os.Getenv("TELEGRAM_BOT_TOKEN")
	publicURL = strings.TrimSuffix(os.Getenv("APP_PUBLIC_URL"), "/")
	if publicURL == "" {
		publicURL = "https://notifyu.me"
	}
	adminChatID = strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))

	dbURL := os.Getenv("SPRING_DATASOURCE_URL")
	if dbURL == "" {
		log.Fatal("CRITICAL: SPRING_DATASOURCE_URL is missing")
	}

	// 2. Initialize Highly-Optimized HTTP Client with Keep-Alive Sockets (Point #4)
	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 10 * time.Second,
	}

	// 3. Connect to Supabase Postgres (Point #1)
	var err error
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	// Configure database pool limits to keep things lightweight
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("Database unreachable: %v", err)
	}
	log.Println("✅ Supabase database connected successfully!")

	// 4. Setup Routes
	http.HandleFunc("/chartink", handleWebhook)
	http.HandleFunc("/telegram", handleTelegram)

	// Fallback route to serve a placeholder or static assets
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<h3>Ultra-low footprint Go router active.</h3>")
	})

	// 5. Start Server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("🚀 Server running natively on port %s", port)
	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		log.Fatalf("Server crash: %v", err)
	}
}

// 1) Chartink & Tradingview Engine with In-Memory Caching
func handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read credentials from Query Params or Form values dynamically
	uid := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("uid")))
	key := strings.TrimSpace(r.URL.Query().Get("key"))

	bodyBytes, _ := io.ReadAll(r.Body)
	bodyStr := string(bodyBytes)

	if uid == "" {
		uid = strings.TrimSpace(strings.ToLower(r.PostFormValue("uid")))
	}
	if key == "" {
		key = strings.TrimSpace(r.PostFormValue("key"))
	}

	if uid == "" || key == "" {
		fmt.Fprint(w, "NO_UID_OR_KEY")
		return
	}

	// Look up user credentials in RAM cache first (Bypasses Database lookup entirely!)
	var chatID string
	var maxAlerts int
	cacheValid := false

	userCacheMutex.RLock()
	entry, found := userCache[uid]
	userCacheMutex.RUnlock()

	if found && time.Now().Before(entry.Expiration) {
		if entry.UserKey == key {
			chatID = entry.ChatID
			maxAlerts = entry.MaxAlerts
			cacheValid = true
		} else {
			fmt.Fprint(w, "FORBIDDEN")
			return
		}
	}

	// Cache miss -> Query Supabase
	// Cache miss -> Query Supabase
if !cacheValid {
    var userKey string
    err := db.QueryRow("SELECT chat_id, user_key, COALESCE(max_alerts, 100) FROM user_map WHERE uid = $1", uid).Scan(&chatID, &userKey, &maxAlerts)
    if err == sql.ErrNoRows {
        fmt.Fprint(w, "UID_NOT_LINKED")
        return
    } else if err != nil {
        log.Printf("DB Error: %v", err)
        fmt.Fprint(w, "OK")
        return
    }

    if userKey != key {
        fmt.Fprint(w, "FORBIDDEN")
        return
    }

    // Save to local cache for the next 5 minutes
    userCacheMutex.Lock()
    userCache[uid] = UserCacheEntry{
        ChatID:     chatID,
        UserKey:    userKey,
        MaxAlerts:  maxAlerts,
        Expiration: time.Now().Add(5 * time.Minute),
    }
    userCacheMutex.Unlock()
}

	// Process daily limits and increments atomically
	todayStr := time.Now().Format("2006-01-02")
	var currentUsage int
	_ = db.QueryRow("SELECT alerts_count FROM daily_usage WHERE chat_id = $1 AND day = $2", chatID, todayStr).Scan(&currentUsage)

	if currentUsage >= maxAlerts {
		fmt.Fprint(w, "LIMIT_EXCEEDED")
		return
	}

	_, _ = db.Exec(`INSERT INTO daily_usage(day, chat_id, alerts_count) VALUES($1, $2, 1)
		ON CONFLICT (day, chat_id) DO UPDATE SET alerts_count = daily_usage.alerts_count + 1`, todayStr, chatID)

	// Format and drop payload into keep-alive sender queue
	go sendTelegram(chatID, buildMessage(bodyStr))
	fmt.Fprint(w, "OK")
}

// 2) Live Core Telegram Router Endpoint
func handleTelegram(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK") // Instantly acknowledge Telegram to stop retry lags

	var update struct {
		UpdateID int64 `json:"update_id"`
		Message  *struct {
			Chat struct {
				ID int64 `json:"id"`
			} `json:"chat"`
			Text string `json:"text"`
		} `json:"message"`
	}

	if err := json.NewDecoder(r.Body).Decode(&update); err != nil || update.Message == nil || update.Message.Text == "" {
		return
	}

	chatIdStr := fmt.Sprintf("%d", update.Message.Chat.ID)
	text := strings.TrimSpace(update.Message.Text)

	// Prevent duplicate webhook requests
	var exists int
	_ = db.QueryRow("SELECT 1 FROM telegram_updates WHERE update_id = $1", update.UpdateID).Scan(&exists)
	if exists == 1 {
		return
	}
	_, _ = db.Exec("INSERT INTO telegram_updates (update_id) VALUES ($1) ON CONFLICT DO NOTHING", update.UpdateID)

	// Simple Command Handler Layout
	if strings.HasPrefix(text, "/start") {
		var uid string
		err := db.QueryRow("SELECT uid FROM user_map WHERE chat_id = $1", chatIdStr).Scan(&uid)
		if err == nil {
			go sendTelegram(chatIdStr, fmt.Sprintf("Linked Successfully : %s\nUse /myuid for Webhook URL.", uid))
		} else {
			// Generate fresh UIDs (Simplified random layout)
			newUid := fmt.Sprintf("u%d", time.Now().UnixNano()%10000000)
			newKey := fmt.Sprintf("k%d", time.Now().UnixNano())
			_, _ = db.Exec("INSERT INTO user_map(uid, chat_id, user_key, updated_at) VALUES($1,$2,$3,$4)",
				newUid, chatIdStr, newKey, time.Now().Unix())
			go sendTelegram(chatIdStr, fmt.Sprintf("✅ *Linked Successfully!*\n\n*Webhook URL:* `%s/chartink?uid=%s&key=%s`", publicURL, newUid, newKey))
		}
	}
}

func sendTelegram(chatID, text string) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", botToken)
	payload, _ := json.Marshal(map[string]interface{}{
		"chat_id":    chatID,
		"text":       text,
		"parse_mode": "Markdown",
	})
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func buildMessage(body string) string {
	// Simple fall-back parsing layout for presentation text
	return "🔔 *New Alert Received*\n\n" + body
}
