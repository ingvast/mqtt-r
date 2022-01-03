REBOL [
    thanks-to: [
	{Codesnippet from Wayne Cui websocket server}
    ]
]

do %mqtt-client.r

host: 192.168.1.156
port: 9001

connection: open/no-wait/binary rejoin [ tcp:// host ":" port ]

ws-UUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

accept-key: func [ key [ string! binary! ] /local uuid ] [
	uuid: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	enbase checksum/method to-binary join to-string key uuid 'sha1
]

create-send-key: func [ /local key ] [
    key: copy [] loop 16 [ append key random 255 ]
    enbase to-binary send-key
]
    
create-handshake-message: func [ host key ][
    rejoin [
	{GET ws://} host {/mqtt HTTP/1.1} crlf
	{Host: } system/network/host-address crlf
	{Connection: Upgrade} crlf
	{Pragma: no-cache} crlf
	{Cache-Control: no-cache} crlf
	{Upgrade: websocket} crlf
	{Origin: tcp://192.168.1.156} crlf
	{Sec-WebSocket-Version: 13} crlf
	{Sec-WebSocket-Key: } send-key crlf
	{Sec-WebSocket-Protocol: mqtt} crlf
	crlf
    ]
]

key: create-send-key 


insert connection  create-handshake-message host key

wait connection
result: copy connection

if result  [
    result: to-string result
    correct-accept-key: accept-key send-key

    unless parse/all result [
	thru "Sec-WebSocket-Accept:"
	any " "  [
	    correct-accept-key some [ crlf | " " | tab ]
	    | ( throw make error! {Wrong accept key} )
	]
	to end
    ] [
	throw make error! reform [ "Error: websocket handshake. Reteurn string" newline result ]
    ]
]
print "**** passed websocket"

connect connection

    
	
close connection
