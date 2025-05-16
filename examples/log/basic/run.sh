#!/bin/sh

jq -c -n '[
	{time:"2025-05-15T00:27:43.012Z", severity:"info",  msg:"login",  blob:"very long extra info"},
	{time:"2025-05-15T00:27:43.012Z", severity:"error", msg:"logout", blob:"very long extra info"}
]' |
	jq -c '.[]' |
	wazero run ./basic.wasm
