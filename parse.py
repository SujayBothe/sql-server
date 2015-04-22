from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword

def test( str ):
        # define SQL tokens
	selectStmt = Forward()
	selectToken = Keyword("select", caseless=True)
	fromToken   = Keyword("from", caseless=True)

	ident          = Word( alphas, alphanums + "_$" ).setName("identifier")
	#columnName     = Upcase( delimitedList( ident, ".", combine=True ) )
	columnName     = delimitedList( ident, ".", combine=True ) 	
	columnNameList = Group( delimitedList( columnName ) )
	tableName      = delimitedList( ident, ".", combine=True ) 
	#tableName      = Upcase( delimitedList( ident, ".", combine=True ) )
	tableNameList  = Group( delimitedList( tableName ) )

	whereExpression = Forward()
	and_ = Keyword("and", caseless=True)
	or_ = Keyword("or", caseless=True)
	in_ = Keyword("in", caseless=True)

	E = CaselessLiteral("E")
	binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
	arithSign = Word("+-",exact=1)
	realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
		                                                 ( "." + Word(nums) ) ) + 
		    Optional( E + Optional(arithSign) + Word(nums) ) )
	intNum = Combine( Optional(arithSign) + Word( nums ) + 
		    Optional( E + Optional("+") + Word(nums) ) )

	columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
	whereCondition = Group(
	    ( columnName + binop + columnRval ) |
	    ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
	    ( columnName + in_ + "(" + selectStmt + ")" ) |
	    ( "(" + whereExpression + ")" )
	    )
	whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

	# define the grammar
	selectStmt      << ( selectToken + 
		           ( '*' | columnNameList ).setResultsName( "columns" ) + 
		           fromToken + 
		           tableNameList.setResultsName( "tables" ) + 
		           Optional( Group( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("where") )

	simpleSQL = selectStmt

	oracleSqlComment = "--" + restOfLine
	simpleSQL.ignore( oracleSqlComment )

	
	try:
		tokens = simpleSQL.parseString( str )
		#a=tokens.asDict()
		#print a['tables'][0]
		#print a['where'][0][1]
		return tokens.asDict()
		#return tokens
		
	except ParseException, err:
		return err
	

