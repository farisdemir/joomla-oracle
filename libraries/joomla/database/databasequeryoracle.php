<?php
defined('JPATH_BASE') or die;
require_once('databasequery.php');
class JDatabaseQueryElementOracle extends JDatabaseQueryElement{
	public function __construct($name, $elements, $glue = ','){
		parent::__construct($name, $elements, $glue);
	}
}
class JDatabaseQueryOracle extends JDatabaseQuery{
	function select($columns){
		$this->_type = 'select';
		if (is_null($this->_select)) {
			$this->_select = new JDatabaseQueryElementOracle('SELECT', $columns);
		}else {
			$this->_select->append($columns);
		}
		return $this;
	}
	function delete($table = null){
		$this->_type	= 'delete';
		$this->_delete	= new JDatabaseQueryElementOracle('DELETE', null);
		if (!empty($table)) {
			$this->from($table);
		}
		return $this;
	}
	function insert($tables){
		$this->_type	= 'insert';
		$this->_insert	= new JDatabaseQueryElementOracle('INSERT INTO', $tables);
		return $this;
	}

	function update($tables){
		$this->_type = 'update';
		$this->_update = new JDatabaseQueryElementOracle('UPDATE', $tables);
		return $this;
	}

	function from($tables){
		if (is_null($this->_from)) {
			$this->_from = new JDatabaseQueryElementOracle('FROM', $tables);
		}	else {
			$this->_from->append($tables);
		}
		return $this;
	}

	function join($type, $conditions){
		if (is_null($this->_join)) {
			$this->_join = array();
		}
		$this->_join[] = new JDatabaseQueryElementOracle(strtoupper($type) . ' JOIN', $conditions);
		return $this;
	}

	function innerJoin($conditions){
		$this->join('INNER', $conditions);

		return $this;
	}

	function outerJoin($conditions){
		$this->join('OUTER', $conditions);

		return $this;
	}

	function leftJoin($conditions){
		$this->join('LEFT', $conditions);

		return $this;
	}

	function rightJoin($conditions){
		$this->join('RIGHT', $conditions);
		return $this;
	}

	function set($conditions, $glue=','){
		if (is_null($this->_set)) {
			$glue = strtoupper($glue);
			$this->_set = new JDatabaseQueryElementOracle('SET', $conditions, "\n\t$glue ");
		}	else {
			$this->_set->append($conditions);
		}
		return $this;
	}

	function where($conditions, $glue=' AND '){
		if (is_null($this->_where)) {
			$glue = strtoupper($glue);
			$this->_where = new JDatabaseQueryElementOracle('WHERE', $conditions, " $glue ");
		}	else {
			$this->_where->append($conditions);
		}
		return $this;
	}

	function group($columns){
		if (is_null($this->_group)) {
			$this->_group = new JDatabaseQueryElementOracle('GROUP BY', $columns);
		}	else {
			$this->_group->append($columns);
		}
		return $this;
	}

	function having($conditions, $glue='AND'){
		if (is_null($this->_having)) {
			$glue = strtoupper($glue);
			$this->_having = new JDatabaseQueryElementOracle('HAVING', $conditions, " $glue ");
		}else {
			$this->_having->append($conditions);
		}
		return $this;
	}

	function order($columns){
		if (is_null($this->_order)) {
			$this->_order = new JDatabaseQueryElementOracle('ORDER BY', $columns);
		}	else {
			$this->_order->append($columns);
		}
		return $this;
	}

	function showTables($name){
		$this->select('NAME');
		$this->from($name.'..sysobjects');
		$this->where('xtype = \'U\'');
		 return $this;
	}

	function dropIfExists($table_name){
		$this->_type = 'drop';
		$drop_syntax = 'IF EXISTS(SELECT TABLE_NAME FROM'.
                    ' INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = \''
                    .$table_name.'\') DROP TABLE';
                    if (is_null($this->_drop)) {
                    	$this->_drop = new JDatabaseQueryElementOracle($drop_syntax, $table_name);
                    }
                    return $this;
	}

	function renameTable($table_name, &$db, $prefix = null, $backup = null){
		 $this->_type = 'rename';
		 $constraints = array();
		 if(!is_null($prefix) && !is_null($backup)){
		 	$constraints = $this->_get_table_constraints($table_name, $db);
		 }
		 if(!empty($constraints))
		 	$this->_renameConstraints($constraints, $prefix, $backup, $db);
		 if (is_null($this->_rename)) {
		 	$this->_rename = new JDatabaseQueryElementOracle('sp_rename', $table_name);
		 }else {
		 	$this->_rename->append($table_name);
		 }
		 return $this;
	}

	private function _renameConstraints($constraints = array(), $prefix = null, $backup = null, $db){
		foreach($constraints as $constraint){
			$db->setQuery('sp_rename '.$constraint.','.str_replace($prefix, $backup, $constraint));
			$db->query();
			if ($db->getErrorNum()) {
			}
		}
	}

	private function _get_table_constraints($table_name, $db){
		$sql = "SELECT CONSTRAINT_NAME FROM".
				" INFORMATION_SCHEMA.TABLE_CONSTRAINTS".
				" WHERE TABLE_NAME = ".$db->quote($table_name);
		$db->setQuery($sql);
		return $db->loadResultArray();
	}

	function insertInto($table_name, $increment_field=false){
		$this->_type = 'insert_into';
		$this->_insert_into = new JDatabaseQueryElementOracle('INSERT INTO', $table_name);
		return $this;
	}

	function fields($fields){
		if (is_null($this->_fields)) {
			$this->_fields = new JDatabaseQueryElementOracle('(', $fields);
		}else {
			$this->_fields->append($fields);
		}
		return $this;
	}

	function values($values){
		if (is_null($this->_values)) {
			$this->_values = new JDatabaseQueryElementOracle('VALUES (', $values);
		}else {
			$this->_values->append($values);
		}
		return $this;
	}

	function auto_increment($query)
	{
		return $query;
	}

	function castToChar($field){
		return $field;
	}

	function charLength($field){
		return;
	}

	function concat($fields, $separator = null)
   {
     if($separator)
     {
       return 'CONCAT('.implode(',', $fields).')';
     }else{
       return 'CONCAT('.implode(',', $fields).')';
     }
   }

	function length($field)
	{
		return 'LENGTH('.$field.')';
	}

	function now(){
		return;
	}

	public function lock($table_name, &$db){
		return true;
	}

	public function unlock(&$db){
		return true;
	}
}