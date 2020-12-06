REBOL [
]

port: open/binary ssl://192.168.1.156:9001

wss-UUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


msg: rejoin [
    {GET /chat HTTP/1.1} crlf
    {HOST: 192.168.1.156} crlf
    {Upgrade: websocket} crlf
    {Connection: Upgrade} crlf
    {Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==} crlf
    {Sec-WebSocket-Protocol: chat, superchar} crlf
    {Sec-WebSocket-Version: 13} crlf
    {Origin: http://localhost} crlf
    crlf
    ]

insert port msg

wait port
result: copy port
;close port

print result
	
