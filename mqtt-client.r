REBOL [
    title: {MQTT client}
]

int-to-2byte-int: func [ val /local result ][
    result: copy #{0000}
    result/1: shift val 8
    result/2: val and 255
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
    
parse-msg: func [ data ][
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
    print-flags: func [ bits][
	flags: [
	    "Reserved"
	    "Clean start"
	    "Will flag"
	    "Willl QoS1"
	    "Willl QoS2"
	    "Will retain1"
	    "Password flag"
	    "User Name Flag"
	]
	repeat i 8 [
	    if 0 != ( bits and shift/left 1 i - 1) [ print flags/:i ]
	]
    ]
    parse/all data [
	#{10}
	var-lengh-int (len: result ? len )
	string  ( mqtt-string: result )
	copy version skip ( print [ "Version:" to-integer first version ] )
	copy connect-flags skip  ( print-flags to-integer connect-flags/1 )
	int ( keep-alive: result ? keep-alive )
	; Property
	; Only ver 5 var-lengh-int ( property-length: result ? property-length )
	; Payload
	parse-dbg
	string (client-id: result ? client-id )
	string (user: result ? user )
	string (password: result ? password )
	parse-dbg
	to end
    ]
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
	}
	spec [block!]
	/local result pos shift-in int-to-bits name len
    ][
	result: 0
	pos: 0
	shift-in: func  [ n val ] [
	    unless integer? val [
		val: either val [ 1 ][ 0 ]
	    ]
	    result: result or shift/left val pos 
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
		set name word! 
		[ 
		    'bool!  ( shift-in 1 get name )
		    |
		    'uint set len integer! ( shift-in len get name )
		    |
		    'int set len integer! ( shift-in len int-to-bits len get name )
		    ;|
		    ;copy val to end ( throw make error! reform [ "Not knwon argument in spec" val  ] )
		]
		|
		    [ none! | 'none! ] p: (print [p] ) opt [set len integer!] ( shift-in any [ len 1] 0)
	    ]
	]
	unless all [ ok pos == 8 ] [ throw make error! {Spec is ill formed} ]
	
	to-binary to-char result
    ]

    binary-to-spec: func [ 
	{byte is an int which is casted to the bitfield described by spec.
	 the variables in spec are set.
	 Spec describes the bits in byte.
	 Starting from least significant bit
	    [ variable-name1 bool! ; variable name is boolean type
	      variable-name2 uint 2 ; variable name will be set to a uint of 2 bits.
	      variable-name3 int 3 ; variable name will be set to int of three bits
	    ]
	 The variable names should be mapped to variables
	}
	byte [integer! binary! ]
	spec [block!]
	/local
	    shift-out int-ize name len 
    ][
	shift-out: func [ n ] [
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
	byte: to-integer byte
	parse spec [
	    any [ 
		set name skip  
		[ 
		    'bool!  ( set name  1 = shift-out 1 )
		    |
		    'uint set len integer! ( set name shift-out len )
		    |
		    'int set len integer! ( set name int-ize shift-out len len )
		    |
		    none! opt set len integer! ( shift-out len )
		    |
		    copy val to end ( throw make error! reform [ "Not knwon argument in spec" val  ] )
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
	msg-proto: [
	    #{10}
	    length
	    variable-header [
		string "MQTT"
		byte #{04}  ; version
		bits [
		    7 off
		    6 off
		    5 off
		    4 off off
		    2 off
		    1 on
		    0 off
		]
		uint16 10
	    ]
	    payload [
		client-id
	    ]
	]
	
	vh: func [ ][
	    string-to-lenstr "MQTT"
	    to-char protocol-level
	    binary-to-spec 
	]
    
	clean-start-flag: 
	will-flag:
	will-retain-flag:
	password-flag:
	username-flag: none
	QoS-level: 0
	
	parse-msg: func [ data ][
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
	    print-flags: func [ bits][
		flags: [
		    "Reserved"
		    "Clean start"
		    "Will flag"
		    "Willl QoS.1"
		    "Willl QoS.2"
		    "Will retain"
		    "Password flag"
		    "User Name Flag"
		]
		repeat i 8 [
		    if 0 != ( bits and shift/left 1 i - 1) [ print flags/:i ]
		]
	    ]
	    
	    parse/all data [
		#{10} ; Connect message
		var-lengh-int (len: result ? len )
		string  ( mqtt-string: result )
		copy version skip ( print [ "Version:" protocol-level: to-integer first version ] )
		copy connect-flags skip  ( print-flags to-integer connect-flags/1 )
		int ( keep-alive: result ? keep-alive )
		; Property
		; Only ver 5 var-lengh-int ( property-length: result ? property-length )
		; Payload
		parse-dbg
		string (client-id: result ? client-id )
		string (user: result ? user )
		string (password: result ? password )
		parse-dbg
		to end
	    ]
	]
    ]

]

	    
	    test-set-bits: func [
		/local
		bit0 bit1 u1 i1
		spec val
	    ][
		spec: [
		    bit0 bool!
		    u1 uint 3
		    bit1 bool!
		    i1 int 3
		]
		val: ( shift/left 0 0 ) or
		     ( shift/left 5 1 ) or
		     ( shift/left 1 4 ) or
		     ( shift/left 3 5 )
		? val
		binary-to-spec val spec
		? bit0 ? bit1
		? u1 ? i1
	    ]



		
