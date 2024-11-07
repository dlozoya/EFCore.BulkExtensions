using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Oracle.ManagedDataAccess.Client;

namespace EFCore.BulkExtensions.SqlAdapters.Oracle;

/// <summary>
///  Contains a list of methods to generate SQL queries required by EFCore
/// </summary>
public class OracleQueryBuilder : SqlQueryBuilder
{
    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    public static string CreateTableCopy(string existingTableName, string newTableName, bool useTempDb)
    {
        string query = $"CREATE TABLE {newTableName} AS SELECT * FROM {existingTableName} WHERE 1=0";
        return query;
    }

    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="isTempTable"></param>
    /// <returns></returns>
    public override string DropTable(string tableName, bool isTempTable)
    {
        var query = $"DROP TABLE {tableName} PURGE";
        return query;
    }

    /// <summary>
    /// Returns a list of columns for the given table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    public static List<string> GetColumnList(TableInfo tableInfo, OperationType operationType)
    {
        var columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
        var uniqueColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.FirstOrDefault();
        if (!tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity) && tableInfo.HasIdentity &&
            (operationType == OperationType.Insert || tableInfo.IdentityColumnName != uniqueColumnName))
        {
            if (!string.IsNullOrEmpty(tableInfo.IdentityColumnName))
            {
                columnsList.Remove(tableInfo.IdentityColumnName);
            }
        }

        return columnsList;
    }

    /// <summary>
    /// Generates SQL merge statement
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    public static string MergeTable<T>(TableInfo tableInfo, OperationType operationType) where T : class
    {
        var columnsList = GetColumnList(tableInfo, operationType);
        string query;

        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            throw new NotImplementedException($"OperationType {OperationType.InsertOrUpdateOrDelete} is not supported in Oracle.");
        }

        var primaryKey = tableInfo.EntityPKPropertyColumnNameDict.First().Value;
        var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList);
        if (operationType == OperationType.Delete)
        {
            query = $"DELETE FROM {tableInfo.FullTableName} WHERE {primaryKey} IN (SELECT {primaryKey} FROM {tableInfo.FullTempTableName})";
        }
        else
        {
            var updateClause = string.Join(", ", columnsList.Select(col => $"{col} = source.{col}"));
            query = $@"
            MERGE INTO {tableInfo.FullTableName} target
            USING (SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName}) source
            ON (target.{primaryKey} = source.{primaryKey})
            WHEN MATCHED THEN UPDATE SET {updateClause}
            WHEN NOT MATCHED THEN INSERT ({commaSeparatedColumns}) VALUES ({commaSeparatedColumns})";
        }

        return query;
    }

    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        var columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        var query = $"SELECT {SqlQueryBuilder.GetCommaSeparatedColumns(columnsNames)} FROM {tableInfo.FullTempOutputTableName} WHERE {tableInfo.PrimaryKeysPropertyColumnNameDict.Values.First()} IS NOT NULL";
        return query;
    }
    /// <summary>
    /// Generates SQL query to chaeck if a unique constrain exist
    /// </summary>
    /// <param name="tableInfo"></param>

    public static string HasUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaName = tableInfo.Schema ?? "YOUR_SCHEMA"; // Reemplaza "YOUR_SCHEMA" con el esquema apropiado si no está en tableInfo
        var uniqueConstraintName = GetUniqueConstrainName(tableInfo);

        var query = $@"
        SELECT DISTINCT CONSTRAINT_NAME 
        FROM ALL_CONSTRAINTS 
        WHERE CONSTRAINT_TYPE = 'U' 
          AND CONSTRAINT_NAME = '{uniqueConstraintName}'
          AND TABLE_NAME = '{tableName?.ToUpper()}' 
          AND OWNER = '{schemaName.ToUpper()}'
    ";
        return query;
    }

    /// <summary>
    /// Generates SQL query to create a unique constraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CreateUniqueConstrain(TableInfo tableInfo)
    {
        var uniqueConstrainName = GetUniqueConstrainName(tableInfo);
        var uniqueColumns = string.Join(", ", tableInfo.PrimaryKeysPropertyColumnNameDict.Values.Select(c => $"\"{c}\""));
        return $@"ALTER TABLE {tableInfo.FullTableName} ADD CONSTRAINT {uniqueConstrainName} UNIQUE ({uniqueColumns})";
    }

    /// <summary>
    /// Generates SQL query to drop a unique constraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string DropUniqueConstrain(TableInfo tableInfo)
    {
        var uniqueConstrainName = GetUniqueConstrainName(tableInfo);
        return $@"ALTER TABLE {tableInfo.FullTableName} DROP CONSTRAINT {uniqueConstrainName}";
    }

    /// <summary>
    /// Restructures a SQL query for batch commands
    /// </summary>
    /// <param name="sql"></param>
    /// <param name="isDelete"></param>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        return sql.Replace("`", "\"");
    }

    /// <summary>
    /// Generates SQL query to truncate table
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public override string TruncateTable(string tableName)
    {
        return $"TRUNCATE TABLE {tableName}";
    }

    /// <inheritdoc/>
    public override DbParameter CreateParameter(string parameterName, object? parameterValue = null)
    {
        return new OracleParameter(parameterName, parameterValue);
    }

    /// <inheritdoc/>
    public override DbCommand CreateCommand()
    {
        return new OracleCommand();
    }

    /// <inheritdoc/>
    public override DbType Dbtype()
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc/>
    public override void SetDbTypeParam(DbParameter parameter, DbType dbType)
    {
        throw new NotSupportedException();
    }

    private static string Md5Hash(string value)
    {
        using var md5 = MD5.Create();
        var buffer = Encoding.UTF8.GetBytes(value);
        return BitConverter.ToString(md5.ComputeHash(buffer)).Replace("-", string.Empty);
    }
    /// <summary>
    /// Creates UniqueConstrainName
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string GetUniqueConstrainName(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";

        string uniqueConstrainPrefix = "tempUniqueIndex_"; // 16 char length
        string uniqueConstrainNameText = $"{schemaDash}{tableName}_{uniqueColumnNamesDash}";
        if (uniqueConstrainNameText.Length > 64 - (uniqueConstrainPrefix.Length)) // effectively 48 max
        {
            uniqueConstrainNameText = Md5Hash(uniqueConstrainNameText);
        }
        string uniqueConstrainName = uniqueConstrainPrefix + uniqueConstrainNameText;
        return uniqueConstrainName;
    }
}
