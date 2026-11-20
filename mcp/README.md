# pi-velvetronix MCP server

MCP server (stdio), který ovládá kombo přes daemon pi-velvetronix stejně jako webové UI:
přihlásí se přes `/api/login`, připojí se jako ovladač na `/ws` a posílá zprávy přes
`web/src/protocol` (sdílený kód s webem, žádná kopie protokolu).

Nástroje: `vox_status`, `vox_catalog`, `vox_set_sound` (živě, nic neukládá, nemění VOLUME),
`vox_list_songs`, `vox_play_song`, `vox_select_program`, `vox_save_program` (přepíše slot),
`vox_disconnect`.

Ovládat může vždy jen jeden ovladač. Server se připojuje s `?takeover=1`, takže ovládání převezme
i od otevřeného prohlížeče. Prohlížeč mezitím přejde do režimu sledování a změny vidí živě.
Po 8 s bez volání se server odpojí a prohlížeč ovládání zase převezme.

```sh
cd mcp && npm ci && npm run build
claude mcp add -s user velvetronix -e VOX_URL=http://velvetronix.local -e VOX_PASSWORD=… \
  -- node "$PWD/dist/server.mjs"
```
