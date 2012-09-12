<?php
defined('JPATH_BASE') or die();
class JDatabaseOracle extends JDatabase {
	var $name			= 'oracle';
	var $_nullDate		= '0001-01-01 00:00:00';
	var $_nameQuote		= '"';
  var $tolower = true;
  var $returnlobs = true;
	function __construct( $options )	{
		$host		= array_key_exists('host', $options)	? $options['host']		: '';
		$user		= array_key_exists('user', $options)	? $options['user']		: '';
		$password	= array_key_exists('password',$options)	? $options['password']	: '';
		$database	= array_key_exists('database',$options)	? $options['database']	: '';
		$prefix		= array_key_exists('prefix', $options)	? $options['prefix']	: '';
		$select		= array_key_exists('select', $options)	? $options['select']	: true;
        $port       = array_key_exists('port', $options)    ? $options['port']      : '1521';
        $charset    = array_key_exists('charset', $options) ? $options['charset']   : 'AL32UTF8';
        $dateformat = array_key_exists('dateformat', $options) ? $options['dateformat'] : 'RRRR-MM-DD HH24:MI:SS';
		if (!function_exists( 'oci_connect' )) {
			$this->errorNum = 1;
			$this->errorMsg = 'The Oracle adapter "oracle" is not available.';
			return;
		}
		if (!($this->connection = oci_connect($user, $password, "//$host:$port/$database", $charset))) {
			$this->errorNum = 2;
			$this->errorMsg = 'Could not connect to Oracle';
			return;
		}       
        $this->setDateFormat($dateformat);
        $this->dateformat = $dateformat;
        $this->charset = $charset;
		parent::__construct($options);
	}
	function __destruct() {
		$return = false;
		if (is_resource($this->connection)) {
			$return = oci_close($this->connection);
		}
		return $return;
	}
	function test() {
		return (function_exists( 'oci_connect' ));
	}
	function connected()	{
		if(is_resource($this->connection)) {
      return true;
		}
		return false;
	}
	function hasUTF() {
		$verParts = explode( '.', $this->getVersion() );
		return ($verParts[0] > 9 || ($verParts[0] == 9 && $verParts[1] == 2) );
	}
	function setUTF() {
	}
	function getEscaped( $text, $extra = false )	{
        return $text;
	}
	function execute() {
		if (!is_resource($this->connection)) {
			return false;
		}
		if ($this->_debug) {
			$this->_ticker++;
			$this->_log[] = $this->sql;
		}
		$this->errorNum = 0;
		$this->errorMsg = '';
		$this->cursor = oci_execute( $this->sql );

      $this->affectedRows = oci_num_rows( $this->sql );
		return $this->sql;
	}

	function setQuery( $query, $offset = 0, $limit = 0, $prefix='#__' ){
		$sql		= $this->replacePrefix( $query, $prefix );
		$this->sql= oci_parse($this->connection, $sql);
		$this->limit	= (int) $limit;
		$this->offset	= (int) $offset;
	}
    function setVar( $placeholder, &$var, $maxlength=-1, $type=SQLT_CHR ) {
        $this->bounded[$placeholder] = array($var, (int)$maxlength, (int)$type);
    }
    function bindVars() {
        if ($this->bounded) {
            foreach($this->bounded as $placeholder => $params) {
                $variable =& $params[0];
                $maxlength = $params[1];
                $type = $params[2];
                if(!oci_bind_by_name($this->sql, $placeholder, $variable, $maxlength, $type)) {
                    $error = oci_error( $this->sql );
                    $this->errorNum = $error['code'];
                    $this->errorMsg = $error['message']." BINDVARS=$placeholder, $variable, $maxlength, $type";
                    if ($this->_debug) {
                        JError::raiseError(500, 'JDatabaseOracle::query: '.$this->errorNum.' - '.$this->errorMsg );
                    }
                    return false;        
                }
            }
        }
        $this->bounded = '';
        return true;
    }
    function defineVar($placeholder, &$variable, $type=SQLT_CHR) {
        if(!oci_define_by_name($this->sql, $placeholder, $variable, $type)) {
            $error = oci_error( $this->sql );
            $this->errorNum = $error['code'];
            $this->errorMsg = $error['message']." DEFINEVAR=$placeholder, $variable, $type";
            if ($this->_debug) {
                JError::raiseError(500, 'JDatabaseOracle::query: '.$this->errorNum.' - '.$this->errorMsg );
            }
            return false;        
        }    
        return true;
    }

    function setDateFormat($dateformat='DD-MON-RR')  {
        $this->setQuery("alter session set nls_date_format = '$dateformat'");
        if (!$this->execute()) {
            return false;
        }
        $this->dateformat = $dateformat;
        return true;
    }
    

    
    function setCharset($charset='AL32UTF8') {
        return true;
    }
    
    function getCharset() {
        return $this->charset;
    }
    
    function createDescriptor($type) {
        if ($type == OCI_D_FILE || $type == OCI_D_LOB || $type == OCI_D_ROWID)
        {
            return oci_new_descriptor($this->connection, $type);
        }
        return false;
    }

	function getPreparedQuery() {
		return $this->sql;
	}
    
    function getBindVars() {
        return $this->bounded;
    }

	function getAffectedRows() {
		return $this->affectedRows;
	}

	function queryBatch( $abort_on_error=true, $p_transaction_safe = false)	{
		$this->errorNum = 0;
		$this->errorMsg = '';
		if ($p_transaction_safe) {
			$sql = rtrim($sql, '; \t\r\n\0');
			$si = $this->getVersion();
			preg_match_all( "/(\d+)\.(\d+)\.(\d+)/i", $si, $m );
			if ($m[1] >= 4) {
				$sql = 'START TRANSACTION;' . $sql . '; COMMIT;';
			} else if ($m[2] >= 23 && $m[3] >= 19) {
				$sql = 'BEGIN WORK;' . $sql . '; COMMIT;';
			} else if ($m[2] >= 23 && $m[3] >= 17) {
				$sql = 'BEGIN;' . $sql . '; COMMIT;';
			}
		}
		$query_split = $this->splitSql($sql);
		$error = 0;
		foreach ($query_split as $command_line) {
			$command_line = trim( $command_line );
			if ($command_line != '') {
                $this->setQuery($command_line);
                $this->execute();
				if (!$this->cursor) {
					$error = 1;
					$this->errorNum .= oci_error( $this->connection ) . ' ';
					$this->errorMsg .= " SQL=$command_line <br />";
					if ($abort_on_error) {
						return $this->cursor;
					}
				}
			}
		}
		return $error ? false : true;
	}

	function getNumRows( $cursor=null )	{
		return $this->numRows;
	}

	function loadResult() {
		if (!($cursor = $this->execute())) {
			return null;
		}
        $mode = $this->getMode(true);
		$ret = null;
		if ($row = oci_fetch_array( $cursor, $mode )) {
			$ret = $row[0];
		}
        $this->numRows = oci_num_rows( $this->sql );
		oci_free_statement( $cursor );
		return $ret;
	}

	function loadResultArray($numinarray = 0){
		if (!($cursor = $this->execute())) {
			return null;
		}
        $mode = $this->getMode(true);
		$array = array();
		while ($row = oci_fetch_array( $cursor, $mode )) {
			$array[] = $row[$numinarray];
		}
        $this->numRows = oci_num_rows( $this->sql );
		oci_free_statement( $cursor );
		return $array;
	}
	function loadAssoc()	{
        $tolower = $this->tolower;
		if (!($cursor = $this->execute())) {
			return null;
		}
        $mode = $this->getMode();
		$ret = null;
		if ($array = oci_fetch_array( $cursor, $mode )) {
            if ($tolower) {
                foreach($array as $field => $value) {
                    $lowercase = strtolower($field);
                    $array[$lowercase] = $value;
                    unset($array[$field]);
                }
            }
            
			$ret = $array;
		}
        $this->numRows = oci_num_rows( $this->sql );
		oci_free_statement( $cursor );
		return $ret;
	}



	function loadObject()	{
        $tolower = $this->tolower;
        $returnlobs = $this->returnlobs;
		if (!($cursor = $this->execute())) {
			return null;
		}
		$ret = null;
		if ($object = oci_fetch_object( $cursor )) {
		    if ($returnlobs) {
                foreach($object as $field => $value) {
                    if (get_class($value) == 'OCI-Lob') {
                        $object->$field = $value->load();
                    }
                }
            }
            if ($tolower) {
                $obj = new stdClass();
                foreach($object as $field => $value) {
                    $lowercase = strtolower($field);
                    $obj->$lowercase = $value;
                    unset($object->$field);
                }
                unset($value);
                unset($object);
                $object = &$obj;
            }
        	$ret = $object;
		}
          $this->numRows = oci_num_rows( $this->sql );
		oci_free_statement( $cursor );
		return $ret;
	}

	function loadObjectList($key='')	{
        $tolower = $this->tolower;
        $returnlobs = $this->returnlobs;
		if (!($cursor = $this->execute())) {
			return null;
		}
		$array = array();
		while ($row = oci_fetch_object( $cursor )) {
                     
            if ($returnlobs) {
                foreach($row as $field => $value) {
                    if (get_class($value) == 'OCI-Lob') {
                        $row->$field = $value->load();
                    }
                }
            }
            if ($tolower) {
                $obj = new stdClass();
                foreach($row as $field => $value) {
                    $lowercase = strtolower($field);
                    $obj->$lowercase = $value;
                    unset($row->$field);
                }
                unset($value);
                unset($row);
            }
			if ($key) {
                if ($tolower) {
                    $lowercase = strtolower($key);
                    $array[$obj->$lowercase] = $obj;
                } else {
                    $array[$row->$key] = $row;
                }
            } else {
                if ($tolower) {
                    $array[] = $obj;
                } else {
                    $array[] = $row;
                }
			}
		}
    $this->numRows = oci_num_rows( $this->sql );
		oci_free_statement( $cursor );
		return $array;
	}
	function loadRow()	{
		if (!($cursor = $this->execute())) {
			return null;
		}
        $mode = $this->getMode(true); 
		$ret = null;
		if ($row = oci_fetch_array( $cursor, $mode )) {
			$ret = $row;
		}
    $this->numRows = oci_num_rows( $this->sql );
		oci_free_statement( $cursor );
		return $ret;
	}
	function loadRowList($key=null)	{
		if (!($cursor = $this->execute())) {
			return null;
		}
        $mode = $this->getMode(true);
		$array = array();
		while ($row = oci_fetch_array( $cursor, $mode )) {
			if ($key !== null) {
				$array[$row[$key]] = $row;
			} else {
				$array[] = $row;
			}
		}
    $this->numRows = oci_num_rows( $this->sql );
		oci_free_statement( $cursor );
		return $array;
	}
    function loadNextRow() {
        static $cursor;
        if (is_null($cursor)) {
            if (!($cursor = $this->execute())) {
                return null;
            }    
        }
        $mode = $this->getMode(true);
        if ($row = oci_fetch_array($cursor, $mode)) {
            return $row;
        }
        $this->numRows = oci_num_rows($this->sql);
        oci_free_statement($cursor);
        $cursor = null;
        return false;
    }
    function loadNextAssoc() {
        static $cursor;
        if (is_null($cursor)) {
            if (!($cursor = $this->execute())) {
                return null;
            }    
        }
        $mode = $this->getMode();
        $tolower = $this->tolower;
        if ($array = oci_fetch_array($cursor, $mode)) {
            if ($tolower) {
                foreach($array as $field => $value) {
                    $lowercase = strtolower($field);
                    $array[$lowercase] = $value;
                    unset($array[$field]);
                }
            }
            return $array;
        }
        $this->numRows = oci_num_rows($this->sql);
        oci_free_statement($cursor);
        $cursor = null;
        return false;
    }
    function loadNextObject() {
        static $cursor;
        $tolower = $this->tolower;
        $returnlobs = $this->returnlobs;
        if (is_null($cursor)) {
            if (!($cursor = $this->execute())) {
                return null;
            }    
        }
        if ($object = oci_fetch_object($cursor)) {
            if ($returnlobs) {
                foreach($object as $field => $value) {
                    if (get_class($value) == 'OCI-Lob') {
                        $object->$field = $value->load();
                    }
                }
            }
            if ($tolower) {
                $obj = new stdClass();
                foreach($object as $field => $value) {
                    $lowercase = strtolower($field);
                    $obj->$lowercase = $value;
                    unset($object->$field);
                }
                unset($value);
                unset($object);
                $object = &$obj;
            }
            return $object;
        }
        $this->numRows = oci_num_rows( $this->sql );
        oci_free_statement( $cursor );
        $cursor = null;

        return false;
    }
	function insertObject( $table, &$object, $keyName = NULL ) 	{
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
                $values[] = $this->isQuoted( $k ) ? $this->Quote( $v ) : (int) $v;
            } else {
                $values[] = $this->Quote($v);
            }
		}
		$this->setQuery( sprintf( $fmtsql, implode( ",", $fields ) ,  implode( ",", $values ) ) );
		if (!$this->execute()) {
			return false;
		}
		return true;
	}

	function updateObject( $table, &$object, $keyName, $updateNulls=true )	{
		$fmtsql = "UPDATE $table SET %s WHERE %s";
		$tmp = array();
		foreach (get_object_vars( $object ) as $k => $v){
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
        if (!$this->execute()) {
            return false;
        }
		return true;
	}

	function getVersion(){
        $this->setQuery("select value from nls_database_parameters where parameter = 'NLS_RDBMS_VERSION'");
        return $this->loadResult();
	}
	function getCollation(){
		return $this->getCharset();
	}
	function getTableList(){
        $sql = 'SELECT table_name FROM all_tables';
        $this->setQuery($sql);
        return $this->loadResultArray();
	}

	function getTableCreate( $tables )	{
		settype($tables, 'array');
		$result = array();
		foreach ($tables as $tblval) {
			$this->setQuery( "select dbms_metadata.get_ddl('TABLE', '".$tblval."') from dual");
			$statement = $this->loadResult();
			$result[$tblval] = $statement;
		}
		return $result;
	}


    function toLower() {
        $this->tolower = true;
    }
    function toUpper() {
        $this->tolower = false;
    }
    function returnLobValues() {
        $this->returnlobs = true;
    }
    function returnLobObjects() {
        $this->returnlobs = false;
    }
    function getMode($numeric = false) {
        if ($numeric === false) {
            if ($this->returnlobs) {
                $mode = OCI_ASSOC+OCI_RETURN_NULLS+OCI_RETURN_LOBS;
            } else {
                $mode = OCI_ASSOC+OCI_RETURN_NULLS;
            }    
        } else {
            if ($this->returnlobs) {
                $mode = OCI_NUM+OCI_RETURN_NULLS+OCI_RETURN_LOBS;
            } else {
                $mode = OCI_NUM+OCI_RETURN_NULLS;
            }            
        }
        return $mode;
    }
}