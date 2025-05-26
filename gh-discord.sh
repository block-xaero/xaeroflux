#!/usr/bin/env bash
# 1) Paste your new Discord webhook URL here (no /github suffix)
WEBHOOK_URL="https://discord.com/api/webhooks/1362095497946333324/qoKSMfIw1nksyBlYSZ3vIApzR7JREwDn1OIxmTGuDitwdjUCul_TYC-qBBCSDDiZzDYE"

# Optional: only replay commits after this SHA (leave empty to replay all)
START_SHA="ffefd2a267f50b0d6fbde6c164e0f990e4838999"

# Determine git range
if [ -n "$START_SHA" ]; then
  RANGE="$START_SHA..HEAD"
else
  RANGE="HEAD"
fi

git rev-list --reverse "$RANGE" | while IFS= read -r sha; do
    # format commit info
    line=$(git log -1 --format='[%h] %s — %an (<https://github.com/block-xaero/xaeroflux/commit/%H>)' "$sha")
    
    # build JSON payload without jq
    PAYLOAD="{\"content\":\"$(printf '%s' "$line" | sed 's/"/\\"/g')\"}"
    
    # send to Discord
    curl -X POST -H "Content-Type: application/json" \
         -d "$PAYLOAD" \
         "$WEBHOOK_URL"
    
    sleep 0.3
done