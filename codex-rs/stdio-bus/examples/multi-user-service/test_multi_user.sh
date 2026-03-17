#!/bin/bash
# Test multiple concurrent users

set -e

API_URL="http://localhost:8000"

echo "=== Multi-User Codex Test ==="
echo ""

# Function to simulate a user
simulate_user() {
    local user_name=$1
    local session_id=""
    
    echo "[$user_name] Starting session..."
    
    # First request - creates session
    response=$(curl -s -X POST "$API_URL/chat" \
        -H "Content-Type: application/json" \
        -d "{\"message\": \"Hello from $user_name\"}")
    
    session_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
    echo "[$user_name] Got session: $session_id"
    
    # Second request - same session (tests affinity)
    response=$(curl -s -X POST "$API_URL/chat" \
        -H "Content-Type: application/json" \
        -d "{\"message\": \"Second message from $user_name\", \"session_id\": \"$session_id\"}")
    
    echo "[$user_name] Second request completed"
    
    # Third request
    response=$(curl -s -X POST "$API_URL/chat" \
        -H "Content-Type: application/json" \
        -d "{\"message\": \"Third message from $user_name\", \"session_id\": \"$session_id\"}")
    
    echo "[$user_name] Done!"
}

# Run 5 users in parallel
echo "Starting 5 concurrent users..."
echo ""

simulate_user "Alice" &
simulate_user "Bob" &
simulate_user "Charlie" &
simulate_user "Diana" &
simulate_user "Eve" &

# Wait for all to complete
wait

echo ""
echo "=== All users completed ==="
echo ""

# Show sessions
echo "Active sessions:"
curl -s "$API_URL/sessions" | python3 -c "import sys,json; data=json.load(sys.stdin); print(json.dumps(data, indent=2))"
