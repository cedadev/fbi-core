
# Notes on sort order.

Sorting in elastic search and in python is ascii order. This matters if I attempt and ordered list for comparison 
of FBI and filesystem. The is a problem comparing paths as strings verses recursive sorting. The standard orders for 
traversing a filesystem are depth first and breath first. These three sort orders produce 3 orders.

| Path string order |     Depth first  |    Breath first   |
| ----------------- | ---------------- | ----------------- |
| /a/b              |     /a/b         |    /a/b           |
| /a/b.ext          |     /a/b/.c      |    /a/b.ext       |
| /a/b/.c           |     /a/b/c       |    /a/bz          |
| /a/b/c            |     /a/b.ext     |    /a/b/.c        |
| /a/bz             |     /a/bz        |    /a/b/c         |

This is because ascii order of characters like  . / and a
Ascii order 

Dec   Hex     Binary	HTML	Char	Description

Can safely ignore these as not likely to be in a file name .
N    0     00      00000000	&#0;	NUL	Null
...
N    31    1F      00011111	&#31;	US	Unit Separator

Printable punctuation
P    32    20      00100000	&#32;	space	Space 
P    33    21      00100001	&#33;	!	exclamation mark
...
D    45    2D      00101101	&#45;	-	minus
D    46    2E      00101110	&#46;	.	period
N    47    2F      00101111	&#47;	/	slash

numbers
D    48    30      00110000	&#48;	0	zero
...
D    57    39      00111001	&#57;	9	nine

more puctuation
D    58    3A      00111010	&#58;	:	colon
N    59    3B      00111011	&#59;	;	semicolon
N    60    3C      00111100	&#60;	<	less than
N    61    3D      00111101	&#61;	=	equality sign
N    62    3E      00111110	&#62;	>	greater than
N    63    3F      00111111	&#63;	?	question mark
D    64    40      01000000	&#64;	@	at sign

Capital letters
D    65    41      01000001	&#65;	A	 
...
D    90    5A      01011010	&#90;	Z				  

yet more puctuation
N    91    5B      01011011	&#91;	[   left square bracket
N    92    5C      01011100	&#92;	\   backslash
N    93    5D      01011101	&#93;	]   right square bracket
P    94    5E      01011110	&#94;	^   caret / circumflex
D    95    5F      01011111	&#95;	_	underscore
N    96    60      01100000	&#96;	`	grave / accent

lower case letters
D    97    61      01100001	&#97;	a				   
...
D    122   7A      01111010	&#122;	z							    

even more punctation and DEL
N    123   7B      01111011	&#123;	{	left curly bracket
N    124   7C      01111100	&#124;	|	vertical bar
N    125   7D      01111101	&#125;	}	right curly bracket
P    126   7E      01111110	&#126;	~	tilde
N    127   7F      01111111	&#127;	DEL	delete

order for definate 
 - . 0 : @ A _ a 
With slash
 - . / 0 : @ A _ a 

order for possibles
 space ! ( ) + , - . 0 : @ A ^ _ a ~
With slash
 space ! ( ) + , - . / 0 : @ A ^ _ a ~

Assuming  filenames do not start with: space ! ( ) + , - then the only character that is likely to mess thing.

