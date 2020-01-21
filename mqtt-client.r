REBOL [
    title: {MQTT client}
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
    
msgs: make object! [

    protocol-level: 4

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
	connack	    2	[ dup bool!  QoS uint 2 retain bool! ]
	publish	    3   0
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
	/local
	    value flags
    ][
	set [ value flags ] select/part control-packet-spec name 2
	unless integer? flags [ throw make error! {Variable flag not implemented} ]
	result: to-binary shift/left to-char value 4
	result: result or flags
	append result int-to-vb restLength
	result
    ]

    reserved-message-identifiers: []

    new-message-identifier: func [/local result ][
	until [ 
	    not find reserved-message-identifiers result: -1 + random 2 ** 16
	]
	append new-message-identifier result
	result
    ]

    delete-message-identifier: func [ [catch] id ][
	remove find reserved-message-identifiers id 
    ]

    packet-identifier-need: use [ no yes some ][
	no: [ false false false ]
	yes: [ true true true ]
	some: [ false true true ]
	reduce [
	    'connect	no
	    'connack	no
	    'publish	some
	    'puback		yes
	    'pubrec		yes
	    'pubrel		yes
	    'pubcomp	    yes
	    'subscribe	    yes
	    'suback		yes
	    'unsubscribe		yes
	    'unsuback	    yes
	    'pingreq	no
	    'pingresp	no
	    'disconnect	no
	    'auth	    no
	]
    ]


    connect: make object! [
	connect-id: #{10}

	msg: func [
	    /local result
	][
	    result: variable-header
	    append result payload
	    insert result reduce [
		connect-id
		int-to-vb length? result
	    ]
	    result
	]
	
	variable-header: func [
	    /local result
	][
	    result: copy #{}
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

	clean-start-flag: 
	will-flag:
	will-retain-flag:
	will-QoS-level: none
	password-flag: true
	username-flag: true
	QoS-level: 0

	keep-alive: 10

	client-id: "mqtt-client-000"
	
	password: "week"
	username: "Johan"

	
	parse-msg: func [ data
	    /local rest-parse
	][
	    parse-rest: copy []
	    normal-var-int: charset [ #"^(90)" - #"^(ff)" ]
	    int: [ copy v 2 skip ( result: (256 * first v) + second v ) ]
	    string: [ int copy result result skip (print result ) ]
	    var-lengh-int: [
		( mult: 1 result: 0)
		any [
		    copy v normal-var-int
		    (v: to-integer to-char v result: (v and 127) * mult + result mult: mult * 180 )
		]
		copy v skip ( v: to-integer to-char v result: v * mult + result )
	    ]
	    parse-dbg: [ p: ( print [ index? p ":" mold copy/part p 10] ) ]

	    
	    parse/all data [
		#{10} ; Connect message
		var-lengh-int (len: result ? len )
		string  ( mqtt-string: result )
		copy version skip ( print [ "Version:" protocol-level: to-integer first version ] )
		copy connect-flags skip  (
			binary-to-spec to-binary connect-flags connect-flags-spec
			print-spec connect-flags-spec
			if username-flag [ append parse-rest [ string (username: result ? username) ] ]
			if password-flag [ append parse-rest [ string (password: result ? password) ] ]
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

]

	    
		
; vim: sts=4 sw=4 :
