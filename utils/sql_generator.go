package utils

func GenerateDataLoaderSql(filepath string, table_name string) string {
	sql := "LOAD DATA INFILE '" + filepath + "' INTO TABLE " + table_name + " FIELDS TERMINATED BY '\t' ENCLOSED BY '\"' LINES TERMINATED BY '\n';"
	return sql
}

func GenerateSelectIntoFileSql(select_sql string, filepath string, delim string, enclose string) string {
	sql := select_sql + " INTO OUTFILE '" + filepath + "'" + " FIELDS TERMINATED BY '" + delim + "' ENCLOSED BY '" + enclose + "' LINES TERMINATED BY '\n';"
	return sql
}
