REBOL [
    title: {MQTT client}
    doc: {
	Using:
	    >> m: open-mqtt/custom tcp://the-mqtt-server.com [ QoS-level: 0 ]
	    A object for mqtt-communication is returned.
	    It will be connected to the server. If failure, none is returned.
	    >> m/subscribe "/post/1" :print
    }
    author: "Johan Ingvast"
]

int-to-2byte-int: func [ val [integer!] /local result ][
    result: copy #{0000}
    result/1: to-char shift val 8
    result/2: to-char val and 255
    result
]

string-to-lenstr: func [ str /local result ][
    result: int-to-2byte-int length? str
    append result str
    return result
]
    

vb-to-int: func [
    {Returns the variable length number as specified in MQTT v5 spec.}
    [catch]
    vb [binary!] {Vb must start at the variable byte integer}
    /ptr {Retruns a block with number as first element and a binary following number}
    /local
	multiplier value encodedByte limit
] [
    multiplier: 1
    value: 0
    limit: 128 ** 3
    until [
	if multiplier > limit [
	    throw make error! "Malformed variable byte"
	]

	encodedByte: first+ vb
	value: (encodedByte and 127) * multiplier + value
	multiplier: multiplier * 128

	encodedByte and 128 = 0
    ]
    either ptr [
	reduce [ value vb ]
    ][
	value
    ]
]
	
int-to-vb: func [
    {Transforms number to binary of variable length}
    x
    /local
	encodedByte vb
][
    vb: copy #{}
    until [
	encodedByte: mod x 128
	x: to-integer x / 128
	if x > 0 [
	    encodedByte: encodedByte or 128
	]
	append vb to-char encodedByte

	x == 0
    ]
    return vb
]
    
mqtt: make object! [

    protocol-level: 4

    hline: does [ "****************************************************" ]

    spec-to-binary: func [ 
	[catch]
	{The variables in bitfield described by spec are casted to an int
	 Starting from least significant bit
	    [ variable-name1 bool! ; variable name is boolean type
	      variable-name2 uint 2 ; variable name will be set to a uint of 2 bits.
	      variable-name3 int 3 ; variable name will be set to int of three bits
	    ]
	 The variable names should be mapped to variables

	As binary the information is Little Endian.
	    Ex.  With the spec
		[	a bool! 
		    b uint 16
		    c bool
		]
	    The information will be stored as
		byte    1		    |  2
		bits    0 1 2 3 4 5 6 7 | 0 1 2 3 4 5 6 7
			a|b0          b6              bF|c
	}
	spec [block!]
	/local bin pos shift-in int-to-bits name len result ok
    ][
	bin: 0
	pos: 0
	shift-in: func  [ n val ] [
	    unless integer? val [
		val: either val [ 1 ][ 0 ]
	    ]
	    bin: bin or shift/left val pos 
	    pos: pos + n
	]

	int-to-bits: func [ len val ][
	    either negative? val [
		(shift/left 1 len ) + val 
	    ][
		val
	    ]
	]
	ok: parse spec [
	    any [ (name: len: none )
		[ none! | 'none! ] opt [set len integer!] ( shift-in any [ len 1] 0)
		|
		set name word! 
		[ 
		    'bool!  ( shift-in 1 get name )
		    |
		    'uint set len integer! ( shift-in len get name )
		    |
		    'int set len integer! ( shift-in len int-to-bits len get name )
		]
	    ]
	]
	unless ok [ throw make error! rejoin [ {Spec is ill formed at:} mold here ] ]
	unless 0 = mod pos 8 [ throw make error! reform [{Spec is not a multiple of eight:} pos ] ]
	
	result: copy #{}
	while [ pos > 0 ] [
	    append result to-char bin and 255
	    bin: shift bin 8
	    pos: pos - 8
	]
	result
    ]

    binary-to-spec: func [ 
	{byte is an int or binary which is casted to the bitfield described by spec.
	 the variables in spec are set.
	 Spec describes the bits in byte.
	 Starting from least significant bit
	    [ variable-name1 bool! ; variable name is boolean type
	      variable-name2 uint 2 ; variable name will be set to a uint of 2 bits.
	      variable-name3 int 3 ; variable name will be set to int of three bits
	    ]
	 The variable names should be mapped to variables
	As binary the information is Little Endian.
	Ex.  With the spec
	    [	a bool! 
		b uint 16
		c bool
	    ]
	The information will be stored as
	    byte    1		    |  2
	    bits    0 1 2 3 4 5 6 7 | 0 1 2 3 4 5 6 7
		    a|b0          b6              bF|c
						   
	}
	byte [integer! binary! ]
	spec [block!]
	/local
	    shift-out int-ize name len  ok here pos
    ][
	shift-out: func [ n ] [
	    pos: pos + n
	    also 
		byte and (( shift/left 1 n ) - 1)
		byte: shift byte n
	]
	int-ize: func [ x len ][
	    ; Make an int out of the number x the int fits in len bits
	    either 0 < ( shift x len - 1) [
		x - ( shift/left 1 len )
	    ][
		x
	    ]
	]
	if binary? byte [
	    byte: byte/1 + (256 * ( (any [ byte/2 0]) + ( 256 * any [ byte/3 0 ] ) ))
	]
	pos: 0
	ok: parse spec [
	    any [  here:
		[ 'none! | none!] opt [ set len integer! ] ( shift-out any [ len 1 ])
		|
		set name skip  
		[ 
		    'bool!  ( set name  1 = shift-out 1 )
		    |
		    'uint set len integer! ( set name shift-out len )
		    |
		    'int set len integer! ( set name int-ize shift-out len len )
		]
	    ]
	]
	unless 0 = mod pos 8 [ throw make error! reform [{Spec is not a multiple of eight:} pos ] ]
	unless ok [ throw make error! rejoin [ {Spec is ill formed at:} mold here ] ]
    ]

    print-spec: func [ spec ][
	parse spec [ 
	    any [ (name: len: none )
		[ none! | 'none! ]  opt integer! 
		|
		set name word!  (prin rejoin [ name ": " ] )
		[ 
		    'bool!  ( print get name )
		    |
		    'uint integer! ( print get name )
		    |
		    'int integer! ( print get name )
		]
	    ]
	]
    ]
    
    ; Fixed header
    control-packet-spec: [
	; name	    value   flags
	reserved    0	0
	connect	    1	0
	connack	    2	0
	publish	    3   [   dup-flag	    bool!
			    QoS-level	    uint    2
			    retain-flag	    bool!    ]
	puback	    4	0
	pubrec	    5	0
	pubrel	    6	2
	pubcomp	    7	0
	subscribe   8	2
	suback	    9	0
	unsubscribe 10	2
	unsuback    11	0
	pingreq	    12	0
	pingresp    13	0
	disconnect  14	0
	auth	    15	0    ; protocol-level 5
    ]

    fixed-header: func [
	[catch]
	name 
	restLength
	ctx
	/local
	    value flags result
    ][
	set [ value flags ] select/skip control-packet-spec name 3
	either  block? flags [
	    ;throw make error! {Variable flag not implemented} 
	    result: bind/copy flags ctx
	    append result [ value uint 4 ]
	    result: spec-to-binary result
	][
	    result: shift/left value 4
	    result: to-binary to-char result or flags
	]
	append result int-to-vb restLength
	result
    ]

    reserved-message-identifiers: []

    new-message-identifier: func [ data /local result ][
	until [ 
	    not find/skip reserved-message-identifiers result: to-integer -1 + random 2 ** 16 2
	]
	append reserved-message-identifiers reduce [ result data ]
	result
    ]
    delete-message-identifier: func [ [catch] id ][
	remove/part find/skip reserved-message-identifiers id 2 2
    ]
    delete-message-data: func [ id ][ select/skip reserved-message-identifiers id 2 ]


    default: make object! [
	type-name: none
	string: func [ /local result ] [
	    result: reform [
		type-name 
	    ]   
	]

	msg: func [ /local result ][
	    result: variable-header
	    append result payload

	    insert result fixed-header type-name length? result self
	    result
	]


;TODO: Structure should be moved up one level.
	packet-identifier-need: use [ no yes some ][
	    no:	    reduce [ false false false ]
	    yes:    reduce [ true true true ]
	    some:   reduce [ false true true ]
	    reduce  [
		'connect	no
		'connack	no
		'publish	some
		'puback		yes
		'pubrec		yes
		'pubrel		yes
		'pubcomp	yes
		'subscribe	yes
		'suback		yes
		'unsubscribe	yes
		'unsuback	yes
		'pingreq	no
		'pingresp	no
		'disconnect	no
		'auth		no
	    ]
	]
	
	QoS-level: none
	packet-identifier: none
	variable-header-start: func [
	    /local  index
	][
	    index: 1 + any [ QoS-level 0 ] 
	    either pick select packet-identifier-need type-name index [
		int-to-2byte-int packet-identifier: new-message-identifier self
	    ][
		copy #{}
	    ]
	]
	
	variable-header: func [
	    [catch]
	    /local result
	][
	    variable-header-start
	]

	respond: func [
	    /local repsond-messages 
	][
	    respond-messages: reduce [ ; message and responsetype depending on QoS
		'connect	[connack]
		'connack	[]
		'publish	[puback pubrec ]
		'puback		[]
		'pubrec		[pubrel]
		'pubrel		[pubcomp]
		'pubcomp	[]
		'subscribe	[suback]
		'suback		[]
		'unsubscribe	[unsuback]
		'unsuback	[]
		'pingreq	[pingresp]
		'pingresp	[]
		'disconnect	[]
		'auth		[]
	    ]
	]

	payload: func [
	    [catch]
	    /local result
	][
	    throw  make error! reform [ type-name {is not implemented}]
	]

	parse-msg: func [
	    [catch]
	    data
	    /local rest-parse
	][
	    throw  make error! reform [ type-name {is not implemented}]
	]
    ]

    connect: make default [
	type-name: 'connect

	string: func [ /local result ] [
	    result: reform [
		type-name  tab client-id tab QoS-level newline
		{Protocol level:} tab protocol-level newline
		{user:} tab username {password:} tab password
	    
	    ]   
	]

	variable-header: func [
	    /local result
	][
	    result: variable-header-start
	    append result string-to-lenstr "MQTT"
	    append result to-char protocol-level
	    append result spec-to-binary connect-flags-spec
	    append result int-to-2byte-int keep-alive
	    if protocol-level > 4 [
		; SHould be all Connect Properties (3.1.2.11)
		throw make error! {Protocol 5 is not yet implemented}
	    ]
	    result
	]

	payload: func [
	    /local result
	][
	    result: copy #{}
	    append result string-to-lenstr client-id
	    if will-flag [
		throw make error! {Will properties are not implemented}
		append result will-properties
	    ]
	    if username-flag [
		append result string-to-lenstr username
	    ]
	    if password-flag [
		append result string-to-lenstr password
	    ]
	    result
	]

	connect-flags-spec: [
	    none!  ; Reserved
	    clean-start-flag bool! 
	    will-flag  bool!
	    will-QoS-level uint 2
	    will-retain-flag bool!
	    password-flag bool!
	    username-flag bool!
	]

	clean-start-flag: true
	will-flag:
	will-retain-flag:
	will-QoS-level: none
	password-flag: true
	username-flag: true
	QoS-level: 0

	keep-alive: 10

	client-id: does [ client-id: join "mqtt-client-" random 10'000 ]
	
	password: "week"
	username: "Johan"
	
	parse-msg: func [ data
	    /local rest-parse
		int string
	][
	    parse-rest: copy []
	    normal-var-int: charset [ #"^(80)" - #"^(ff)" ]
	    int: [ copy v 2 skip ( result: (256 * first v) + second v ) ]
	    string: [ int copy result result skip  ]
	    var-lengh-int: [
		( mult: 1 result: 0)
		any [
		    copy v normal-var-int
		    (v: to-integer to-char v result: (v and 127) * mult + result mult: mult * 128 )
		]
		copy v skip ( v: to-integer to-char v result: v * mult + result )
	    ]
	    parse-dbg: [ p: ( print [ index? p ":" mold copy/part p 10] ) ]

	    
	    parse/all data [
		#{10} ; Connect message
		var-lengh-int (len: result )
		string  ( mqtt-string: result )
		copy version skip ( print [ "Version:" protocol-level: to-integer first version ] )
		copy connect-flags skip  (
			binary-to-spec to-binary connect-flags connect-flags-spec
			print-spec connect-flags-spec
			if username-flag [ append parse-rest [ string (username: result ) ] ]
			if password-flag [ append parse-rest [ string (password: result ) ] ]
		)
		int ( keep-alive: result ? keep-alive )
		; Property
		; Only ver 5 var-lengh-int ( property-length: result ? property-length )
		; Payload
		string (client-id: result ? client-id )
		parse-rest
		end
	    ]
	]
    ]

    connack: make default [
	type-name: 'connack

	session-present: none
	connect-return-code: none

	string: func [ /local result ] [
	    result: reform [
		type-name
	    ]   
	    append result reform [ newline
		select connect-return-codes-table any [ connect-return-code no-connect ]
		]
	    result
	]

	variable-header-spec: [
	    none! 7
	    session-present bool!
	    connect-return-code uint 8
	]

	connect-return-codes-table: [
	    0 "connection accepted"
	    1 "connection refused unaccceptable protocol version"
	    2 "Connection refused, identifier rejected"
	    3 "Connection refused, Server unavailable"
	    4 "Connection refused, bad user name or password (malformed)"
	    5 "Connection refused, not authorized"
	    
	    no-connect "Connect return code not set"
	]

	parse-msg: func [ data
	    /local rest-parse
		int string len
	][
	    parse-rest: copy []
	    normal-var-int: charset [ #"^(80)" - #"^(ff)" ]
	    int: [ copy v 2 skip ( result: (256 * first v) + second v ) ]
	    string: [ int copy result result skip (print result ) ]
	    var-lengh-int: [
		( mult: 1 result: 0)
		any [
		    copy v normal-var-int
		    (v: to-integer to-char v result: (v and 127) * mult + result mult: mult * 128 )
		]
		copy v skip ( v: to-integer to-char v result: v * mult + result )
	    ]
	    parse-dbg: [ p: ( print [ index? p ":" mold copy/part p 10] ) ]

	    
	    id: first+ data
	    ;print [ "Msg type" pick control-packet-spec (3 * shift id 4) + 1 ]
	    parse/all data [
		var-lengh-int (
		    len: result 
		    unless len = 2 [
			throw make error! {Wrong length of connack package}
		    ]
		)
		
		copy variable-header-data [2 skip] (
		    binary-to-spec to-binary variable-header-data variable-header-spec 
		)
	    ]
	]
    ]

    publish: make default [
	type-name: 'publish

	string: func [ /result ][
	    result: reform [
		type-name  QoS-level tab
		{Retain: } retain-flag newline
		{Topic:} topic newline
		{Payload: (} length? payload {)} payload tab
		"(" mold to-string payload ")"
	    ]
	    result
	]

	topic: none
	payload: #{}

	dup-flag: false
	QoS-level: 0
	retain-flag: false
	packet-identifier: false
    
	variable-header: func [
	    /local result
	][
	    result: variable-header-start
	    append result string-to-lenstr topic
	    result
	]

	parse-msg: func [
	    data
	    /local rest-parse p
		int string result  v
	][
	    parse-rest: copy []
	    int: [ copy v 2 skip ( result: (256 * first v) + second v ) ]
	    string: [ int  copy result result skip ]
	    normal-var-int: charset [ #"^(80)" - #"^(ff)" ]
	    var-lengh-int: [
		( mult: 1 result: 0)
		any [
		    copy v normal-var-int
		    (v: to-integer to-char v result: (v and 127) * mult + result mult: mult * 128 )
		]
		copy v skip ( v: to-integer to-char v result: v * mult + result )
	    ]
	    parse-dbg: [ p: ( print [ index? p ":" mold copy/part p 10] ) ]

	    
	    id: first+ data
	    parse/all data [ 
		var-lengh-int ( len: result  )
		p: ( unless len = length? p [
		    throw make error! probe reform [ 
			{The publish packet has wrong length. Found } length? p { should be } len]
		] )
		
		string ( topic: to-string result )
		(
		    possible-packet-identifier:
			either QoS-level > 0 [
			    [ int ( packet-identifier: result ) ]
			][
			    []
			]
		)
		possible-packet-identifier
		copy payload to end ( payload: to-binary payload )
	    ]
	]
    ]

    puback: make default [
	type-name: 'puback
	; only if OoS-level > 0
    ]

    subscribe: make default [
	type-name: 'subscribe

	string: func [ /local result ][
	    result: reform [
		type-name tab  packet-identifier
		newline
		mold new-line/skip topics-qos on 2
	    ]
	]

	topics-qos: []

	add-topic: func [ topic qos ][
	    append topics-qos topic
	    append topics-qos qos
	]

	variable-header: func [ /local result ][
	    result: variable-header-start
	]

	payload: func[ /local result QoS ][
	    result: copy #{}
	    foreach [t q] topics-qos  [
		append result string-to-lenstr t
		append result to-char q
	    ]
	    result
	]

	parse-msg: func [
	    data
	    /local rest-parse p int string
	][
	    parse-rest: copy []

	    int: [ copy v 2 skip ( result: (256 * first v) + second v ) ]
	    string: [ int copy result result skip (print result ) ]
	    normal-var-int: charset [ #"^(80)" - #"^(ff)" ]
	    var-lengh-int: [
		( mult: 1 result: 0)
		any [
		    copy v normal-var-int
		    (v: to-integer to-char v result: (v and 127) * mult + result mult: mult * 128 )
		]
		copy v skip ( v: to-integer to-char v result: v * mult + result )
	    ]
	    parse-dbg: [ p: ( print [ index? p ":" mold copy/part p 10] ) ]

	    id: first+ data
	    
	    ;print [ "Msg type" pick control-packet-spec (3 * shift id 4) + 1 ]
	    unless type-name = pick control-packet-spec (3 * shift id 4) + 1 [
		throw make error! reform [{Not a} type-name {subscribe message}]
	    ]

	    parse/all data [ 
		var-lengh-int (
		    len: result 
		)
		p: ( unless len = length? p [ throw make error! {The publish packet has wrong length} ] )
		
		string ( topic: to-string result )
		(
		    possible-packet-identifier:
			either QoS-level > 0 [
			    [ int ( packet-identifier: result ) ]
			][
			    []
			]
		)
		possible-packet-identifier
		copy payload to end ( payload: to-binary payload )
	    ]
	]
    ]

    suback: make default [
	type-name: 'suback

	string: func [ /local result ][
	    result: reform [
		type-name  packet-identifier
		newline
		mold QoS-response
	    ]
	]

	QoS-response: []
	packet-identifier: none
	parse-msg: func [
	    data
	    /local rest-parse p
		int string
	][
	    parse-rest: copy []

	    int: [ copy v 2 skip ( result: (256 * first v) + second v ) ]

	    id: first+ data
	    
	    ;print [ "Msg type" pick control-packet-spec (3 * shift id 4) + 1 ]
	    unless type-name = pick control-packet-spec (3 * shift id 4) + 1 [
		throw make error! reform [{Not a} type-name {subscribe message}]
	    ]

	    parse/all data [ 
		var-lengh-int ( len: result )
		p: ( unless len = length? p [ throw make error! {The publish packet has wrong length} ] )
		
		; Variable header
		int ( packet-identifier: result )

		; Payload
		some [
		    copy QoS skip ( append QoS-response to-integer QoS/1 )
		]
	    ]
	    delete-message-identifier packet-identifier
	]

    ]
]

connect: func [
    address [ url! port! ]
][
    either port? address [
	c: address
    ] [
	c: open/no-wait/binary url
    ]
    
    connect-message: make mqtt/connect [
	username: "johan"
	password: "saby"
	username-flag: true 
	password-flag: true
    ]
    insert c m: connect-message/msg
    print "Connection request inserted"
    wait c
    connect-acc: copy c
    either connect-acc [
	connack-message: make mqtt/connack []
	connack-message/parse-msg connect-acc
	print mqtt/hline
	print connack-message/string
    ][
	print {Server disconnected, check connect parameters}
	connect-message/parse-msg connect-message/msg
	halt
    ]
    c
]
    
cont: func [] [
    pub: make mqtt/publish [ topic: "/asdf" payload: "aaa" ]
    sub: make mqtt/subscribe [ add-topic "/rebol/#" 0 ]
    insert c sub/msg
    print mqtt/hline
    print sub/string
    forever [
	d: wait [ 2.0 c ]
	switch d compose [
	    (c) [
		data: copy c
		unless data [
		    print {Bye}
		    break ;; forever
		]
		id: first data
		type-name: pick mqtt/control-packet-spec (3 * shift id 4) + 1
		p: make mqtt/:type-name [ parse-msg data ]
		print mqtt/hline
		print p/string
	    ]
	    (none) [ ; timeout
		pub/payload: to-binary now/precise
		insert c dbg: pub/msg
	    ]
	]
    ]
    close c
]
		
; vim: sts=4 sw=4 :
