package ni.danny.datax.plugin.writer.oracleseqwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.druid.sql.parser.ParserException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

public final class WriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(WriterUtil.class);

    //TODO 切分报错
    public static List<Configuration> doSplit(Configuration simplifiedConf,
                                              int adviceNumber) {

        List<Configuration> splitResultConfigs = new ArrayList<Configuration>();

        int tableNumber = simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK);

        //处理单表的情况
        if (tableNumber == 1) {
            //由于在之前的  master prepare 中已经把 table,jdbcUrl 提取出来，所以这里处理十分简单
            for (int j = 0; j < adviceNumber; j++) {
                splitResultConfigs.add(simplifiedConf.clone());
            }

            return splitResultConfigs;
        }

        if (tableNumber != adviceNumber) {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                    String.format("您的配置文件中的列配置信息有误. 您要写入的目的端的表个数是:%s , 但是根据系统建议需要切分的份数是：%s. 请检查您的配置并作出修改.",
                            tableNumber, adviceNumber));
        }

        String jdbcUrl;
        List<String> preSqls = simplifiedConf.getList(Key.PRE_SQL, String.class);
        List<String> postSqls = simplifiedConf.getList(Key.POST_SQL, String.class);

        List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK,
                Object.class);

        for (Object conn : conns) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration.from(conn.toString());
            jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            sliceConfig.remove(Constant.CONN_MARK);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            for (String table : tables) {
                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set(Key.TABLE, table);
                tempSlice.set(Key.PRE_SQL, renderPreOrPostSqls(preSqls, table));
                tempSlice.set(Key.POST_SQL, renderPreOrPostSqls(postSqls, table));

                splitResultConfigs.add(tempSlice);
            }

        }

        return splitResultConfigs;
    }

    public static List<String> renderPreOrPostSqls(List<String> preOrPostSqls, String tableName) {
        if (null == preOrPostSqls) {
            return Collections.emptyList();
        }

        List<String> renderedSqls = new ArrayList<String>();
        for (String sql : preOrPostSqls) {
            //preSql为空时，不加入执行队列
            if (StringUtils.isNotBlank(sql)) {
                renderedSqls.add(sql.replace(Constant.TABLE_NAME_PLACEHOLDER, tableName));
            }
        }

        return renderedSqls;
    }

    public static void executeSqls(Connection conn, List<String> sqls, String basicMessage,DataBaseType dataBaseType) {
        Statement stmt = null;
        String currentSql = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                currentSql = sql;
                DBUtil.executeSqlWithoutResultSet(stmt, sql);
            }
        } catch (Exception e) {
            throw RdbmsException.asQueryException(dataBaseType,e,currentSql,null,null);
        } finally {
            DBUtil.closeDBResources(null, stmt, null);
        }
    }

    public static String getWriteTemplate(List<OracleColumnCell> columnHolders, String valueHolder, String writeMode, DataBaseType dataBaseType, boolean forceUseUpdate) {
        boolean isWriteModeLegal = writeMode.trim().toLowerCase().startsWith("insert")
                || writeMode.trim().toLowerCase().startsWith("merge")
                || writeMode.trim().toLowerCase().startsWith("update")
                || writeMode.trim().toLowerCase().startsWith("delete");

        if (!isWriteModeLegal) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    String.format("您所配置的 writeMode:%s 错误. 因为DataX 目前仅支持merge,update,insert,delete 方式. 请检查您的配置并作出修改.", writeMode));
        }
        String writeDataSqlTemplate = null;
        if(writeMode.trim().toLowerCase().startsWith("insert")){

            StringBuilder columnsStr = new StringBuilder();
            StringBuilder valueStr = new StringBuilder();

            boolean firstColumn = true;
            boolean firstValue = true;

            for(OracleColumnCell columnCell:columnHolders){

                if(ColumnType.SEQ.equals(columnCell.getColumnType())){
                    if(firstColumn){
                        firstColumn = false;
                    }else{
                        columnsStr.append(",");
                    }
                    if(firstValue){
                        firstValue = false;
                    }else{
                        valueStr.append(",");
                    }
                    columnsStr.append(columnCell.getColumnName());
                    valueStr.append(columnCell.getSeqName()+".nextval  ");
                }else if(ColumnType.CONST.equals(columnCell.getColumnType())||ColumnType.WHERE_CONST.equals(columnCell.getColumnType())){
                    if(firstColumn){
                        firstColumn = false;
                    }else{
                        columnsStr.append(",");
                    }
                    if(firstValue){
                        firstValue = false;
                    }else{
                        valueStr.append(",");
                    }
                    columnsStr.append(columnCell.getColumnName());
                    valueStr.append(" '"+columnCell.getValue()+"' ");
                }else if(ColumnType.DATE.equals(columnCell.getColumnType())
                        ||ColumnType.WHERE_DATE.equals(columnCell.getColumnType())
                        ||ColumnType.CONST_DATE.equals(columnCell.getColumnType())
                        ||ColumnType.WHERE_CONST_DATE.equals(columnCell.getColumnType())){

                    if(firstColumn){
                        firstColumn = false;
                    }else{
                        columnsStr.append(",");
                    }
                    if(firstValue){
                        firstValue = false;
                    }else{
                        valueStr.append(",");
                    }
                    columnsStr.append(columnCell.getColumnName());

                    if(ColumnType.CONST_DATE.equals(columnCell.getColumnType())
                            ||ColumnType.WHERE_CONST_DATE.equals(columnCell.getColumnType())){

                        valueStr.append("  to_date('"+columnCell.getValue()+"','"+columnCell.getFormat()+"') ");
                    }else{
                        valueStr.append("  to_date("+valueHolder+",'"+columnCell.getFormat()+"') ");
                    }

                }else{
                    if(firstColumn){
                        firstColumn = false;
                    }else{
                        columnsStr.append(",");
                    }
                    if(firstValue){
                        firstValue = false;
                    }else{
                        valueStr.append(",");
                    }
                    columnsStr.append(columnCell.getColumnName());
                    valueStr.append(valueHolder);
                }
            }

            writeDataSqlTemplate = new StringBuilder()
                    .append("INSERT INTO %s (").append(columnsStr)
                    .append(") VALUES(").append(valueStr)
                    .append(")")
                    .toString();

        }else if(writeMode.trim().toLowerCase().startsWith("merge")){

            StringBuilder usingSqlStr = new StringBuilder().append(" select ");
            StringBuilder onSqlStr = new StringBuilder();
            StringBuilder updateSqlStr = new StringBuilder();
            StringBuilder insertColumnsStr = new StringBuilder();
            StringBuilder insertValuesStr = new StringBuilder();

            boolean usingSqlFirst = true;
            boolean onSqlFirst = true;
            boolean insertColumnsFirst = true;
            boolean insertValuesFirst = true;
            boolean updateSqlFirst = true;

            for(OracleColumnCell columnCell:columnHolders){

                if(insertColumnsFirst){
                    insertColumnsFirst =false;
                }else{
                    insertColumnsStr.append(",");
                }
                insertColumnsStr.append(" "+columnCell.getColumnName()+"  \r\n");

                if(ColumnType.WHERE.equals(columnCell.getColumnType())){
                    if(usingSqlFirst){
                        usingSqlFirst=false;
                    }else {
                        usingSqlStr.append(", ");
                    }

                    usingSqlStr.append(" "+valueHolder+" as "+columnCell.getColumnName()+" ");

                    if(onSqlFirst){
                        onSqlFirst =false;
                    }else{
                        onSqlStr.append("and ");
                    }

                    onSqlStr.append(" tab1."+columnCell.getColumnName()+"=tab2."+columnCell.getColumnName()+" \r\n");


                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }

                    insertValuesStr.append(" tab2."+columnCell.getColumnName()+" ");

                }else if(ColumnType.WHERE_CONST.equals(columnCell.getColumnType())){

                    if(onSqlFirst){
                        onSqlFirst =false;
                    }else{
                        onSqlStr.append(" and ");
                    }

                    onSqlStr.append(" tab1."+columnCell.getColumnName()+"='"+columnCell.getValue()+"' \r\n");


                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }

                    insertValuesStr.append(" '"+columnCell.getValue()+"' ");

                }else if(ColumnType.WHERE_DATE.equals(columnCell.getColumnType())){

                    if(onSqlFirst){
                        onSqlFirst =false;
                    }else{
                        onSqlStr.append(" and ");
                    }

                    onSqlStr.append(" tab1."+columnCell.getColumnName()+"= to_date("+valueHolder+",'"+columnCell.getFormat()+"')  \r\n");

                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }

                    insertValuesStr.append("   to_date("+valueHolder+",'"+columnCell.getFormat()+"')   ");

                }else if(ColumnType.SEQ.equals(columnCell.getColumnType())){

                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }

                    insertValuesStr.append(" "+columnCell.getSeqName()+".nextval ");
                }else if(ColumnType.CONST.equals(columnCell.getColumnType())){

                    if(updateSqlFirst){
                        updateSqlFirst = false;
                    }else{
                        updateSqlStr.append(" , ");
                    }
                    updateSqlStr.append(" tab1."+columnCell.getColumnName()+" = '"+columnCell.getValue()+"' \r\n");

                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }
                    insertValuesStr.append(" '"+columnCell.getValue()+"' ");

                }else if(ColumnType.DATE.equals(columnCell.getColumnType())){

                    if(updateSqlFirst){
                        updateSqlFirst = false;
                    }else{
                        updateSqlStr.append(" , ");
                    }
                    updateSqlStr.append(" tab1."+columnCell.getColumnName()+" = TO_DATE("+valueHolder+",'"+columnCell.getFormat()+"') \r\n");

                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }
                    insertValuesStr.append("  TO_DATE("+valueHolder+",'"+columnCell.getFormat()+"')  ");

                }else if(ColumnType.VALUE.equals(columnCell.getColumnType())){
                    if(usingSqlFirst){
                        usingSqlFirst=false;
                    }else {
                        usingSqlStr.append(", ");
                    }
                    usingSqlStr.append(" "+valueHolder+" as "+columnCell.getColumnName()+" ");

                    if(updateSqlFirst){
                        updateSqlFirst = false;
                    }else{
                        updateSqlStr.append(" , ");
                    }
                    updateSqlStr.append(" tab1."+columnCell.getColumnName()+" = tab2."+columnCell.getColumnName()+" \r\n");

                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }
                    insertValuesStr.append(" tab2."+columnCell.getColumnName()+" ");

                }else if(ColumnType.CONST_DATE.equals(columnCell.getColumnType())){

                    if(updateSqlFirst){
                        updateSqlFirst = false;
                    }else{
                        updateSqlStr.append(" , ");
                    }
                    updateSqlStr.append(" tab1."+columnCell.getColumnName()+" = TO_DATE('"+columnCell.getValue()+"','"+columnCell.getFormat()+"') \r\n");

                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }
                    insertValuesStr.append("  TO_DATE('"+columnCell.getValue()+"','"+columnCell.getFormat()+"')  ");

                }else if(ColumnType.WHERE_CONST_DATE.equals(columnCell.getColumnType())){

                    if(onSqlFirst){
                        onSqlFirst =false;
                    }else{
                        onSqlStr.append(" and ");
                    }

                    onSqlStr.append(" tab1."+columnCell.getColumnName()+"= to_date('"+columnCell.getValue()+"','"+columnCell.getFormat()+"')  \r\n");


                    if(insertValuesFirst){
                        insertValuesFirst =false;
                    }else{
                        insertValuesStr.append(", ");
                    }

                    insertValuesStr.append("   to_date('"+columnCell.getValue()+"','"+columnCell.getFormat()+"')   ");

                }
            }


            writeDataSqlTemplate = new StringBuilder()
                    .append(" MERGE INTO %s tab1 ")
                    .append(" USING ( ")
                    .append(usingSqlStr)
                    .append(" FROM dual) tab2 ")
                    .append(" ON ( ")
                    .append(onSqlStr)
                    .append(" ) ")
                    .append(" WHEN MATCHED THEN ")
                    .append(" UPDATE SET  ")
                    .append(updateSqlStr)
                    .append(" WHEN NOT MATCHED THEN ")
                    .append(" INSERT (")
                    .append(insertColumnsStr)
                    .append(") VALUES(")
                    .append(insertValuesStr)
                    .append(") ")
                    .toString();

        }else if(writeMode.trim().toLowerCase().startsWith("update")){
            StringBuilder setSqlStr = new StringBuilder();
            StringBuilder whereSqlStr = new StringBuilder();

            boolean firstSetSql = true;
            boolean firstWhereSql = true;

            for(OracleColumnCell columnCell:columnHolders){

                if(ColumnType.SEQ.equals(columnCell.getColumnType())){
                    if(firstSetSql){
                        firstSetSql = false;
                    }else{
                        setSqlStr.append(", ");
                    }
                    setSqlStr.append(columnCell.getColumnName()+"="+columnCell.getSeqName()+".nextval ");
                }else if(ColumnType.CONST_DATE.equals(columnCell.getColumnType())){

                    if(firstSetSql){
                        firstSetSql = false;
                    }else{
                        setSqlStr.append(", ");
                    }
                    setSqlStr.append(columnCell.getColumnName()+"= to_date('"+columnCell.getValue()+"','"+columnCell.getFormat()+"')");

                }else if(ColumnType.DATE.equals(columnCell.getColumnType())){

                    if(firstSetSql){
                        firstSetSql = false;
                    }else{
                        setSqlStr.append(", ");
                    }
                    setSqlStr.append(columnCell.getColumnName()+"= to_date("+valueHolder+",'"+columnCell.getFormat()+"')");

                }else if(ColumnType.CONST.equals(columnCell.getColumnType())){

                    if(firstSetSql){
                        firstSetSql = false;
                    }else{
                        setSqlStr.append(", ");
                    }
                    setSqlStr.append(columnCell.getColumnName()+"= '"+columnCell.getValue()+"'");

                }else if(ColumnType.VALUE.equals(columnCell.getColumnType())){
                    if(firstSetSql){
                        firstSetSql = false;
                    }else{
                        setSqlStr.append(", ");
                    }
                    setSqlStr.append(columnCell.getColumnName()+"="+valueHolder);
                }else if(ColumnType.WHERE.equals(columnCell.getColumnType())
                ||ColumnType.WHERE_DATE.equals(columnCell.getColumnType())
                ||ColumnType.WHERE_CONST.equals(columnCell.getColumnType())
                ||ColumnType.WHERE_CONST_DATE.equals(columnCell.getColumnType())){
                    if(firstWhereSql){
                        firstWhereSql = false;
                    }else{
                        whereSqlStr.append(" and ");
                    }
                    if(ColumnType.WHERE.equals(columnCell.getColumnType())){
                        whereSqlStr.append(" "+columnCell.getColumnName()+"="+valueHolder+" ");
                    }else if(ColumnType.WHERE_DATE.equals(columnCell.getColumnType())){
                        whereSqlStr.append(" "+columnCell.getColumnName()+"= to_date("+valueHolder+",'"+columnCell.getFormat()+"') ");
                    }else if(ColumnType.WHERE_CONST_DATE.equals(columnCell.getColumnType())){
                        whereSqlStr.append(" "+columnCell.getColumnName()+"= to_date('"+columnCell.getValue()+"','"+columnCell.getFormat()+"') ");
                    }else if(ColumnType.WHERE_CONST.equals(columnCell.getColumnType())){
                        whereSqlStr.append(" "+columnCell.getColumnName()+"='"+columnCell.getValue()+"' ");
                    }
                }
            }

            writeDataSqlTemplate = new StringBuilder()
                    .append("UPDATE %s set ")
                    .append(setSqlStr)
                    .append(" where ")
                    .append(whereSqlStr)
                    .toString();
        }else if(writeMode.trim().toLowerCase().startsWith("delete")){

            StringBuilder whereSqlStr = new StringBuilder();
            boolean firstWhere = true;

            for(OracleColumnCell columnCell:columnHolders){
                if(ColumnType.WHERE.equals(columnCell.getColumnType())||
                        ColumnType.WHERE_DATE.equals(columnCell.getColumnType())||
                        ColumnType.WHERE_CONST.equals(columnCell.getColumnType())||
                        ColumnType.WHERE_CONST_DATE.equals(columnCell.getColumnType())){
                    if(firstWhere){
                        firstWhere = false;
                    }else{
                        whereSqlStr.append(" and ");
                    }

                    if(ColumnType.WHERE.equals(columnCell.getColumnType())){
                        whereSqlStr.append( columnCell.getColumnName() +"="+ valueHolder+" ");
                    }else if(ColumnType.WHERE_CONST.equals(columnCell.getColumnType())){
                        whereSqlStr.append( columnCell.getColumnName() +"='"+columnCell.getValue()+"' ");
                    }else if(ColumnType.WHERE_CONST_DATE.equals(columnCell.getColumnType())){
                        whereSqlStr.append(" "+columnCell.getColumnName()+"= to_date('"+columnCell.getValue()+"','"+columnCell.getFormat()+"') ");
                    }else if(ColumnType.WHERE_DATE.equals(columnCell.getColumnType())){
                        whereSqlStr.append( columnCell.getColumnName() +"=to_date("+valueHolder+",'"+columnCell.getFormat()+"') ");
                    }
                }
            }

            writeDataSqlTemplate = new StringBuilder()
                    .append("delete from %s  ")
                    .append(" where ")
                    .append(whereSqlStr)
                    .toString();
        }

        return writeDataSqlTemplate;
    }

    public static void preCheckPrePareSQL(Configuration originalConfig, DataBaseType type) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> preSqls = originalConfig.getList(Key.PRE_SQL,
                String.class);
        List<String> renderedPreSqls = WriterUtil.renderPreOrPostSqls(
                preSqls, table);

        if (null != renderedPreSqls && !renderedPreSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].",
                    StringUtils.join(renderedPreSqls, ";"));
            for(String sql : renderedPreSqls) {
                try{
                    DBUtil.sqlValid(sql, type);
                }catch(ParserException e) {
                    throw RdbmsException.asPreSQLParserException(type,e,sql);
                }
            }
        }
    }

    public static void preCheckPostSQL(Configuration originalConfig, DataBaseType type) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());
        String table = connConf.getList(Key.TABLE, String.class).get(0);

        List<String> postSqls = originalConfig.getList(Key.POST_SQL,
                String.class);
        List<String> renderedPostSqls = WriterUtil.renderPreOrPostSqls(
                postSqls, table);
        if (null != renderedPostSqls && !renderedPostSqls.isEmpty()) {

            LOG.info("Begin to preCheck postSqls:[{}].",
                    StringUtils.join(renderedPostSqls, ";"));
            for(String sql : renderedPostSqls) {
                try{
                    DBUtil.sqlValid(sql, type);
                }catch(ParserException e){
                    throw RdbmsException.asPostSQLParserException(type,e,sql);
                }

            }
        }
    }


}
