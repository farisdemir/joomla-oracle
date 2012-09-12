<?php
defined('JPATH_BASE') or die();
class JDatabaseOracle extends JDatabase{
	var $name = 'oracle';
	var $_nullDate = '1999-01-01 00:00:00';
	var $_nameQuote		= '"';
	var $_prepared			= '';
	var $_bounded            = '';
	var $_charset            = '';
	var $_affectedRows       = '';
	var $_numRows       = '';
	var $_tolower = true;
	var $_returnlobs = true;
	var $_commitMode = null;
	function __construct( $options ){
		$host		= array_key_exists('host', $options)	? $options['host']		: '';
		$user		= array_key_exists('user', $options)	? $options['user']		: '';
		$password	= array_key_exists('password',$options)	? $options['password']	: '';
		$database	= array_key_exists('database',$options)	? $options['database']	: '';
		$prefix		= array_key_exists('prefix', $options)	? $options['prefix']	: 'JOS_';
		$select		= array_key_exists('select', $options)	? $options['select']	: true;
		$port       = array_key_exists('port', $options)    ? $options['port']      : '1521';
		$charset    = array_key_exists('charset', $options) ? $options['charset']   : 'AL32UTF8';
		$dateformat = array_key_exists('dateformat', $options) ? $options['dateformat'] : 'RRRR-MM-DD HH24:MI:SS';
		$timestampformat = array_key_exists('timestampformat', $options) ? $options['timestampformat'] : 'RRRR-MM-DD HH24:MI:SS';

		if (!$this->test()) {
			$this->_errorNum = 1;
			$this->_errorMsg = 'The Oracle adapter "oracle" is not available.';
			return;
		}

		if (!($this->_connection = @ oci_connect($user, $password, "//$host:$port/$database", $charset))) {
			$this->_errorNum = 2;
			$this->_errorMsg = 'Could not connect to Oracle';
			return;
		}

		$this->_charset = $charset;
		$this->setDateFormat($dateformat);
		$this->setTimestampFormat($timestampformat);
		$this->setCommitMode(OCI_COMMIT_ON_SUCCESS);
		parent::__construct($options);
	}
	function __destruct(){
		$return = false;
		if (is_resource($this->_connection)) {
			$return = oci_close($this->_connection);
		}
		return $return;
	}

	function test(){
		return (function_exists('oci_connect'));
	}

	function connected(){
		if(is_resource($this->_connection)) {
			return true;
		}
		return false;
	}

	function select($database)	{
		$this->_connection;
		return true;
	}

	function hasUTF(){
		$verParts = explode('.', $this->getVersion());
		if ($verParts[0] > 9 || ($verParts[0] == 9 && $verParts[1] == 2)) {
			if (strripos($this->_charset, 'utf8') !== false) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	function setUTF(){
		return $this->setCharset();
	}

	function getEscaped($text, $extra = false)	{
		return $text;
	}

	function query()	{
		if (!is_resource($this->_connection)) {
			return false;
		}
			$this->setQuery($this->_sql);
			$this->bindVars();

		if ($this->_debug) {
			$this->_ticker++;
			$this->_log[] = $this->_sql;
		}
		$this->_errorNum = 0;
		$this->_errorMsg = '';
		$this->_cursor = oci_execute($this->_prepared, $this->_commitMode);

		if (!$this->_cursor){
			$error = oci_error($this->_prepared);
			$this->_errorNum = $error['code'];
			$this->_errorMsg = $error['message']." SQL=$this->_sql";

			if ($this->_debug) {
				JError::raiseError(500, 'JDatabaseOracle::query: '.$this->_errorNum.' - '.$this->_errorMsg);
			}
			return false;
		}
		$this->_affectedRows = oci_num_rows($this->_prepared);
		return $this->_prepared;
	}

	function setQuery($sql, $offset = 0, $limit = 0, $prefix='#__'){
		$this->_sql		= $this->replacePrefix($sql, $prefix);
		$this->_prepared= oci_parse($this->_connection, $this->_sql);
		$this->_limit	= (int) $limit;
		$this->_offset	= (int) $offset;
	}
	
	function getQuery($new = false){
		if ($new) {
			jimport('joomla.database.databasequeryOracle');
			return new JDatabaseQueryOracle;
		} else {
			return $this->_prepared;
		}
	}

	function setVar($placeholder, &$var, $maxlength=-1, $type=SQLT_CHR){
		$this->_bounded[$placeholder] = array($var, (int)$maxlength, (int)$type);
	}

	function bindVars(){
		if ($this->_bounded){
			foreach($this->_bounded as $placeholder => $params){
				$variable =& $params[0];
				$maxlength = $params[1];
				$type = $params[2];
				if(!oci_bind_by_name($this->_prepared, $placeholder, $variable, $maxlength, $type)){
					$error = oci_error($this->_prepared);
					$this->_errorNum = $error['code'];
					$this->_errorMsg = $error['message']." BINDVARS=$placeholder, $variable, $maxlength, $type";
					if ($this->_debug){
						JError::raiseError(500, 'JDatabaseOracle::query: '.$this->_errorNum.' - '.$this->_errorMsg );
					}
					return false;
				}
			}
		}
		$this->_bounded = '';
		return true;
	}

	function defineVar($placeholder, &$variable, $type=SQLT_CHR){
		if(!oci_define_by_name($this->_prepared, $placeholder, $variable, $type)){
			$error = oci_error($this->_prepared);
			$this->_errorNum = $error['code'];
			$this->_errorMsg = $error['message']." DEFINEVAR=$placeholder, $variable, $type";

			if ($this->_debug){
				JError::raiseError(500, 'JDatabaseOracle::query: '.$this->_errorNum.' - '.$this->_errorMsg);
			}
			return false;
		}

		return true;
	}

	function setDateFormat($dateformat = 'DD-MON-RR'){
		$this->setQuery("alter session set nls_date_format = '$dateformat'");
		if (!$this->query()) {
			return false;
		}
		return true;
	}

	function getDateFormat(){
		$this->setQuery("select value from nls_session_parameters where parameter = 'NLS_DATE_FORMAT'");
		return $this->loadResult();
	}

	function setTimestampFormat($timestampformat = 'DD-MON-RR HH.MI.SSXFF AM'){
		$this->setQuery("alter session set nls_timestamp_format = '$timestampformat'");
		if (!$this->query()) {
			return false;
		}
		return true;
	}

	function getTimestampFormat(){
		$this->setQuery("select value from nls_session_parameters where parameter = 'NLS_TIMESTAMP_FORMAT'");
		return $this->loadResult();
	}

	function setCharset($charset = 'AL32UTF8'){
		return false;
	}

	function getCharset(){
		return $this->_charset;
	}

	function getDatabaseCharset(){
		$this->setQuery("select value from nls_database_parameters where parameter = 'NLS_CHARACTERSET'");
		return $this->loadResult();
	}

	function createDescriptor($type){
		if ($type == OCI_D_FILE || $type == OCI_D_LOB || $type == OCI_D_ROWID)
		{
			return oci_new_descriptor($this->_connection, $type);
		}
		return false;
	}

	function getPreparedQuery(){
		return $this->_prepared;
	}

	function getBindVars(){
		return $this->_bounded;
	}

	function getAffectedRows(){
		return $this->_affectedRows;
	}

	function queryBatch($abort_on_error = true, $p_transaction_safe = false)
	{
		$this->_errorNum = 0;
		$this->_errorMsg = '';
		if ($p_transaction_safe) {
			$this->_sql = rtrim($this->_sql, '; \t\r\n\0');
			$si = $this->getVersion();
			preg_match_all("/(\d+)\.(\d+)\.(\d+)/i", $si, $m);
			if ($m[1] >= 4) {
				$this->_sql = 'START TRANSACTION;' . $this->_sql . '; COMMIT;';
			} else if ($m[2] >= 23 && $m[3] >= 19) {
				$this->_sql = 'BEGIN WORK;' . $this->_sql . '; COMMIT;';
			} else if ($m[2] >= 23 && $m[3] >= 17) {
				$this->_sql = 'BEGIN;' . $this->_sql . '; COMMIT;';
			}
		}
		$query_split = $this->splitSql($this->_sql);
		$error = 0;
		foreach ($query_split as $command_line) {
			$command_line = trim($command_line);
			if ($command_line != '') {
				$this->setQuery($command_line);
				$this->query();
				if (!$this->_cursor) {
					$error = 1;
					$this->_errorNum .= oci_error($this->_connection) . ' ';
					$this->_errorMsg .= " SQL=$command_line <br />";
					if ($abort_on_error) {
						return $this->_cursor;
					}
				}
			}
		}
		return $error ? false : true;
	}

	function explain(){
		$temp = $this->_sql;
		$this->setQuery("SELECT TABLE_NAME
                         FROM USER_TABLES
                         WHERE USER_TABLES.TABLE_NAME = 'PLAN_TABLE'");
		$result = $this->loadResult();
		if (!$result){
			$this->setQuery('CREATE TABLE "PLAN_TABLE" (
                                          "STATEMENT_ID"  VARCHAR2(30),
                                          "TIMESTAMP"  DATE,
                                          "REMARKS"  VARCHAR2(80),
                                          "OPERATION"  VARCHAR2(30),
                                          "OPTIONS"  VARCHAR2(30),
                                          "OBJECT_NODE"  VARCHAR2(128),
                                          "OBJECT_OWNER"  VARCHAR2(30),
                                          "OBJECT_NAME"  VARCHAR2(30),
                                          "OBJECT_INSTANCE"  NUMBER(22),
                                          "OBJECT_TYPE"  VARCHAR2(30),
                                          "OPTIMIZER"  VARCHAR2(255),
                                          "SEARCH_COLUMNS"  NUMBER(22),
                                          "ID"  NUMBER(22),
                                          "PARENT_ID"  NUMBER(22),
                                          "POSITION"  NUMBER(22),
                                          "COST"  NUMBER(22),
                                          "CARDINALITY"  NUMBER(22),
                                          "BYTES"  NUMBER(22),
                                          "OTHER_TAG"  VARCHAR2(255),
                                          "OTHER"  LONG)'
                                          );
                                          if (!($cur = $this->query())) {
                                          	return null;
                                          }
		}

		$this->_sql = "EXPLAIN PLAN FOR $temp";
		$this->setQuery($this->_sql);
		if (!($cur = $this->query())) {
			return null;
		}
		$first = true;
		$buffer = '<table id="explain-sql">';
		$buffer .= '<thead><tr><td colspan="99">'.$this->getQuery().'</td></tr>';
		$this->setQuery("SELECT * FROM PLAN_TABLE");
		if (!($cur = $this->query())) {
			return null;
		}
		while ($row = oci_fetch_assoc($cur)) {
			if ($first) {
				$buffer .= '<tr>';
				foreach ($row as $k=>$v) {
					if ($k == 'STATEMENT_ID' || $k == 'REMARKS' || $k == 'OTHER_TAG' || $k == 'OTHER') {
						continue;
					}
					$buffer .= '<th>'.$k.'</th>';
				}
				$buffer .= '</tr>';
				$first = false;
			}
			$buffer .= '</thead><tbody><tr>';
			foreach ($row as $k=>$v) {
				if ($k == 'STATEMENT_ID' || $k == 'REMARKS' || $k == 'OTHER_TAG' || $k == 'OTHER') {
					continue;
				}
				$buffer .= '<td>'.$v.'</td>';
			}
			$buffer .= '</tr>';
		}
		$buffer .= '</tbody></table>';

		$this->setQuery("DELETE PLAN_TABLE");

		if (!($cur = $this->query())) {
			return null;
		}
		oci_free_statement($cur);

		$this->_sql = $temp;
		$this->setQuery($this->_sql);

		return $buffer;
	}

	function getNumRows($cur = null){
		return $this->_numRows;
	}

	function loadResult(){
		if (!($cur = $this->query())) {
			return null;
		}
		$mode = $this->getMode(true);
		$ret = null;
		if ($row = oci_fetch_array($cur, $mode)) {
			$ret = $row[0];
		}
		$this->_numRows = oci_num_rows($this->_prepared);
		oci_free_statement($cur);
		return $ret;
	}

	function loadResultArray($numinarray = 0){
		if (!($cur = $this->query())) {
			return null;
		}
		$mode = $this->getMode(true);
		$array = array();
		while ($row = oci_fetch_array($cur, $mode)) {
			$array[] = $row[$numinarray];
		}
		$this->_numRows = oci_num_rows($this->_prepared);
		oci_free_statement($cur);
		return $array;
	}

	function loadAssoc(){
		$tolower = $this->_tolower;
		if (!($cur = $this->query())) {
			return null;
		}
		$mode = $this->getMode();
		$ret = null;
		if ($array = oci_fetch_array($cur, $mode)) {
			if ($tolower) {
				$array = array_change_key_case($array, CASE_LOWER);
			}
			$ret = $array;
		}
		$this->_numRows = oci_num_rows($this->_prepared);
		oci_free_statement($cur);
		return $ret;
	}

	function loadAssocList( $key='', $column = null ){
		$tolower = $this->_tolower;
		if (!($cur = $this->query())) {
			return null;
		}

		$mode = $this->getMode();

		$array = array();
		while ($row = oci_fetch_array($cur, $mode)) {
			if ($tolower) {
				$row = array_change_key_case($row, CASE_LOWER);
			}

			if ($key) {
				$array[$row[$key]] = $row;
			} else {
				$array[] = $row;
			}
		}
		//Updates the affectedRows variable with the number of rows returned by the query
		$this->_numRows = oci_num_rows($this->_prepared);
		oci_free_statement($cur);
		return $array;
	}

	function loadObject($className = 'stdClass', $params = null){
		$row = $this->loadAssoc();
		if (is_null($row)) {
			return $row;
		} else {
			if ($className === 'stdClass') {
				return (object) $row;
			} else {
				if (is_null($params)) {
					return new $className($row);
				} else {
					return new $className($row, $params);
				}

			}
		}
	}

	public function loadObjectList($key = '', $className = 'stdClass', $params = null)
	{
		$list = $this->loadAssocList($key);
		if (is_null($list)) {
			return $list;
		}
		foreach($list as $k => $row) {
			if ($className === 'stdClass') {
				$list[$k] = (object) $row;
			} else {
				if (is_null($params)) {
					$list[$k] = new $className($row);
				} else {
					$list[$k] = new $className($row, $params);
				}
			}
		}
		return $list;
	}

	function loadRow(){
		if (!($cur = $this->query())) {
			return null;
		}

		$mode = $this->getMode(true);

		$ret = null;
		if ($row = oci_fetch_array($cur, $mode)) {
			$ret = $row;
		}
		$this->_numRows = oci_num_rows($this->_prepared);
		oci_free_statement($cur);
		return $ret;
	}

	function loadRowList($key=null){
		if (!($cur = $this->query())) {
			return null;
		}
		$mode = $this->getMode(true);
		$array = array();
		while ($row = oci_fetch_array($cur, $mode)) {
			if ($key !== null) {
				$array[$row[$key]] = $row;
			} else {
				$array[] = $row;
			}
		}
		$this->_numRows = oci_num_rows($this->_prepared);
		oci_free_statement($cur);
		return $array;
	}

	function loadNextRow(){
		static $cur;
		if (is_null($cur)) {
			if (!($cur = $this->query())) {
				return null;
			}
		}
		$mode = $this->getMode(true);
		if ($row = oci_fetch_array($cur, $mode)) {
			return $row;
		}
		$this->_numRows = oci_num_rows($this->_prepared);
		oci_free_statement($cur);
		$cur = null;
		return false;
	}

	function loadNextAssoc(){
		static $cur;
		if (is_null($cur)) {
			if (!($cur = $this->query())) {
				return null;
			}
		}
		$mode = $this->getMode();
		$tolower = $this->_tolower;
		if ($array = oci_fetch_array($cur, $mode)) {
			if ($tolower) {
				$array = array_change_key_case($array, CASE_LOWER);
			}
			return $array;
		}
		$this->_numRows = oci_num_rows($this->_prepared);
		oci_free_statement($cur);
		$cur = null;

		return false;
	}

	function loadNextObject($className = 'stdClass', $params = null){
		$row = $this->loadNextAssoc();
		if (is_null($row) || $row === false) {
			return $row;
		} else {
			if ($className === 'stdClass') {
				return (object) $row;
			} else {
				if (is_null($params)) {
					return new $className($row);
				} else {
					return new $className($row, $params);
				}

			}
		}
	}

	function insertObject( $table, &$object, $keyName = NULL ){
		$fmtsql = "INSERT INTO $table ( %s ) VALUES ( %s ) ";
		$fields = array();
		$values = array();
		foreach (get_object_vars( $object ) as $k => $v) {
			if (is_array($v) or is_object($v) or $v === NULL) {
				continue;
			}
			if ($k[0] == '_') {
				continue;
			}
			$fields[] = $k;
			if ( $k == $keyName ) {
				$values[] = $this->nextinsertid($table);
			} else {
				$values[] = $this->Quote($v);
			}
		}
		//$query = sprintf( $fmtsql, implode( ",", $fields ) ,  implode( ",", $values ) );
		//return $query;
		$this->setQuery( sprintf( $fmtsql, implode( ",", $fields ) ,  implode( ",", $values ) ) );
		if (!$this->query()) {
			return false;
		}
		return true;
	}


	function updateObject( $table, &$object, $keyName, $updateNulls=true )
	{
		$fmtsql = "UPDATE $table SET %s WHERE %s";
		$tmp = array();
		foreach (get_object_vars( $object ) as $k => $v)
		{
			if( is_array($v) or is_object($v) or $k[0] == '_' ) {
				continue;
			}
			if( $k == $keyName ) {
				$where = $keyName . '=' . $this->Quote( $v );
				continue;
			}
			if ($v === null)	{
				if ($updateNulls) {
					$val = 'NULL';
				} else {
					continue;
				}
			} else {
				$val = $this->isQuoted( $k ) ? $this->Quote( $v ) : (int) $v;
			}
			$tmp[] = $k . '=' . $val;
		}
		$this->setQuery( sprintf( $fmtsql, implode( ",", $tmp ) , $where ) );
		if (!$this->query()) {
			return false;
		}
		return true;
	}

	function insertid($tableName = null, $primaryKey = null){
		if ($tableName !== null) {
			$sequenceName = $tableName;
			if ($primaryKey) {
				$sequenceName .= "_$primaryKey";
			}
			$sequenceName .= '_SEQ';
			return $this->lastSequenceId($sequenceName);
		}
		return null;
	}

	function lastSequenceId($sequenceName){
		$this->_sql = 'SELECT '.$sequenceName.'.CURRVAL FROM dual';
		$this->setQuery($this->_sql);
		$value = $this->loadResult();
		return $value;
	}

	function nextInsertId($tableName = null, $primaryKey = null){
		if ($tableName !== null) {
			$sequenceName = $tableName;
			if ($primaryKey) {
				$sequenceName .= "_$primaryKey";
			}
			$sequenceName .= '_SEQ';
			return $this->nextSequenceId($sequenceName);
		}
		return null;
	}

	function nextSequenceId($sequenceName){
		$this->_sql = 'SELECT '.$sequenceName.'.NEXTVAL FROM dual';
		$this->setQuery($this->_sql);
		$value = $this->loadResult();
		return $value;
	}

	function getVersion(){
		$this->setQuery("select value from nls_database_parameters where parameter = 'NLS_RDBMS_VERSION'");
		return $this->loadResult();
	}

	function getCollation(){
		return $this->getCharset();
	}

	function getTableList(){
		$this->_sql = 'SELECT table_name FROM all_tables';
		$this->setQuery($this->_sql);
		return $this->loadResultArray();
	}

	function getTableCreate( $tables ){
		settype($tables, 'array'); 
		$result = array();

		foreach ($tables as $tblval) {
			$this->setQuery( "select dbms_metadata.get_ddl('TABLE', '".$tblval."') from dual");
			$statement = $this->loadResult();
			$result[$tblval] = $statement;
		}

		return $result;
	}

	function getTableFields($tables, $typeonly = true)	{
		settype($tables, 'array'); 
		$result = array();
		foreach ($tables as $tblval)	{
			//$tblval = strtoupper($tblval);
			$this->setQuery( "SELECT *
                              FROM ALL_TAB_COLUMNS
                              WHERE table_name = '".$tblval."'");
			$fields = $this->loadObjectList('', false);

			if($typeonly)	{
				foreach ($fields as $field) {
					$result[$tblval][$field->COLUMN_NAME] = preg_replace("/[(0-9)]/",'', $field->DATA_TYPE );
				}
			}else{
				foreach ($fields as $field) {
					$result[$tblval][$field->COLUMN_NAME] = $field;
				}
			}
		}
		return $result;
	}

	function toLower(){
		$this->_tolower = true;
	}
	function toUpper(){
		$this->_tolower = false;
	}

	function returnLobValues(){
		$this->_returnlobs = true;
	}

	function returnLobObjects(){
		$this->_returnlobs = false;
	}

	function getMode($numeric = false)	{
		if ($numeric === false) {
			if ($this->_returnlobs) {
				$mode = OCI_ASSOC+OCI_RETURN_NULLS+OCI_RETURN_LOBS;
			}else {
				$mode = OCI_ASSOC+OCI_RETURN_NULLS;
			}
		} else {
			if ($this->_returnlobs) {
				$mode = OCI_NUM+OCI_RETURN_NULLS+OCI_RETURN_LOBS;
			}else {
				$mode = OCI_NUM+OCI_RETURN_NULLS;
			}
		}
		return $mode;
	}

	function getCommitMode(){
		return $this->_commitMode;
	}
	
	function setCommitMode($commit_mode){
		$this->_commitMode = $commit_mode;
	}

	public function toSQLDate(&$date, $local = false)
	{
		return $date->toSQL($local);
	}
}