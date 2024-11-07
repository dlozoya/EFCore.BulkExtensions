﻿using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using NetTopologySuite.Geometries;
using EFCore.BulkExtensions.SqlAdapters.Oracle;
using Oracle.ManagedDataAccess.Client;

namespace EFCore.BulkExtensions.SqlAdapters.Oracle;

/// <inheritdoc/>
public class OracleAdapter : ISqlOperationsAdapter
{
    private OracleQueryBuilder ProviderSqlQueryBuilder => new OracleQueryBuilder();

    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task InsertAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected static async Task InsertAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        bool isAsync, CancellationToken cancellationToken)
    {
        // Configuración inicial de las entidades y preservación de identidades
        tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities);

        // Abrimos la conexión a la base de datos de forma asíncrona o síncrona según el caso
        if (isAsync)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.OpenConnection();
        }

        var connection = (OracleConnection)context.Database.GetDbConnection();

        try
        {
            // Configuramos OracleBulkCopy para la operación de inserción masiva
            using var oracleBulkCopy = new OracleBulkCopy(connection);
            SetOracleBulkCopyConfig(oracleBulkCopy, tableInfo);

            // Preparamos el DataTable a partir de las entidades, utilizando un lector de datos si está disponible
            var dataTable = GetDataTable(context, type, entities, oracleBulkCopy, tableInfo);
            IDataReader? dataReader = tableInfo.BulkConfig.DataReader;

            // Ejecutamos la operación de inserción masiva
            if (dataReader == null)
            {
                oracleBulkCopy.WriteToServer(dataTable);
            }
            else
            {
                oracleBulkCopy.WriteToServer(dataReader);
            }
        }
        finally
        {
            // Cerramos la conexión de forma asíncrona o síncrona según el caso
            if (isAsync)
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
            else
            {
                context.Database.CloseConnection();
            }
        }

        // Restablecemos el orden de identidad en las entidades si no se ha creado una tabla de salida
        if (!tableInfo.CreateOutputTable)
        {
            tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities, reset: true);
        }

    }
    /// <inheritdoc/>
    public void Merge<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress)
        where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task MergeAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress,
        CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    protected Task MergeAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress,
        bool isAsync, CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();



    }
    /// <inheritdoc/>
    public void Read<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task ReadAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo)
    {
        var sqlTruncateTable = ProviderSqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
        context.Database.ExecuteSqlRaw(sqlTruncateTable);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        var sqlTruncateTable = ProviderSqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
        await context.Database.ExecuteSqlRawAsync(sqlTruncateTable, cancellationToken).ConfigureAwait(false);
    }
    #endregion
    #region Connection

    internal static async Task<(DbConnection, bool)> OpenAndGetOracleConnectionAsync(DbContext context,
        bool isAsync, CancellationToken cancellationToken)
    {
        bool oonnectionOpenedInternally = false;
        var connection = context.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
        {
            if (isAsync)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                connection.Open();
            }
            oonnectionOpenedInternally = true;
        }
        return (connection, oonnectionOpenedInternally);
    }

    internal static async Task<(bool, bool)> CheckHasExplicitUniqueConstrainAsync(DbContext context, TableInfo tableInfo,
        bool isAsync, CancellationToken cancellationToken)
    {
        string countUniqueConstrain = OracleQueryBuilder.HasUniqueConstrain(tableInfo);

        (DbConnection connection, bool connectionOpenedInternally) = await OpenAndGetOracleConnectionAsync(context, isAsync, cancellationToken).ConfigureAwait(false);

        bool hasUniqueConstrain = false;
        using (var command = connection.CreateCommand())
        {
            command.CommandText = countUniqueConstrain;
            command.Transaction = context.Database.CurrentTransaction?.GetDbTransaction();

            if (isAsync)
            {
                using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (reader.HasRows)
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        Int64.TryParse(reader[0].ToString(), out long result);
                        hasUniqueConstrain = result == 1;
                    }
                }
            }
            else
            {
                using var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        Int64.TryParse(reader[0].ToString(), out long result);
                        hasUniqueConstrain = result == 1;
                    }
                }
            }
        }
        return (hasUniqueConstrain, connectionOpenedInternally);
    }

    private static OracleBulkCopy GetOracleBulkCopy(OracleConnection oracleConnection, IDbContextTransaction? transaction, BulkConfig config)
    {
        var oracleTransaction = transaction == null ? null : (OracleTransaction)transaction.GetUnderlyingTransaction(config);
        var oracleBulkCopy = new OracleBulkCopy(oracleConnection);

        return oracleBulkCopy;
    }
    /// <summary>
    /// Supports <see cref="OracleBulkCopy"/>
    /// </summary>
    /// <param name="oracleBulkCopy"></param>
    /// <param name="tableInfo"></param>
    private static void SetOracleBulkCopyConfig(OracleBulkCopy oracleBulkCopy, TableInfo tableInfo)
    {
        string destinationTable = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName
                                                              : tableInfo.FullTableName;
        destinationTable = destinationTable.Replace("[", "")
                                           .Replace("]", "");
        oracleBulkCopy.DestinationTableName = destinationTable;

        oracleBulkCopy.NotifyAfter = tableInfo.BulkConfig.NotifyAfter ?? tableInfo.BulkConfig.BatchSize;
        oracleBulkCopy.BulkCopyTimeout = tableInfo.BulkConfig.BulkCopyTimeout ?? oracleBulkCopy.BulkCopyTimeout;
    }

    #endregion
    #region DataTable
    /// <summary>
    /// Supports <see cref="OracleBulkCopy"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="oracleBulkCopy"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static DataTable GetDataTable<T>(DbContext context, Type type, IEnumerable<T> entities, OracleBulkCopy oracleBulkCopy, TableInfo tableInfo)
    {
        DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);

        int sourceOrdinal = 0;
        foreach (DataColumn item in dataTable.Columns)  //Add mapping
        {
            oracleBulkCopy.ColumnMappings.Add(new OracleBulkCopyColumnMapping(sourceOrdinal, item.ColumnName));
            sourceOrdinal++;
        }
        return dataTable;
    }

    /// <summary>
    /// Common logic for two versions of GetDataTable
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    private static DataTable InnerGetDataTable<T>(DbContext context, ref Type type, IEnumerable<T> entities, TableInfo tableInfo)
    {
        var dataTable = new DataTable();
        var columnsDict = new Dictionary<string, object?>();
        var ownedEntitiesMappedProperties = new HashSet<string>();

        var databaseType = SqlAdaptersMapping.GetDatabaseType();
        var isOracle = databaseType == SqlType.Oracle;

        var objectIdentifier = tableInfo.ObjectIdentifier;
        type = tableInfo.HasAbstractList ? entities.ElementAt(0)!.GetType() : type;
        var entityType = context.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
        var entityTypeProperties = entityType.GetProperties();

        var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) ||
                                                                   (tableInfo.BulkConfig.OperationType != OperationType.Read && a.Name == tableInfo.TimeStampPropertyName))
                                                       .ToDictionary(a => a.Name, a => a);

        var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.TargetEntityType.IsOwned())
                                                                   .ToDictionary(a => a.Name, a => a);

        var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                           a.IsForeignKey() &&
                                                                           a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                               .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        var entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict.ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));
        var shadowPropertyColumnNamesDict = entityPropertiesDict.Where(a => a.Value.IsShadowProperty())
                                                                .ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        var discriminatorColumn = GetDiscriminatorColumn(tableInfo);

        foreach (var property in properties)
        {
            var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                && !tableInfo.BulkConfig.SetOutputIdentity
                && tableInfo.DefaultValueProperties.Contains(property.Name);

            if (entityPropertiesDict.ContainsKey(property.Name))
            {
                var propertyEntityType = entityPropertiesDict[property.Name];
                string columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType
                                                 : property.PropertyType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isOracle && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(byte[]);
                    tableInfo.HasSpatialType = true;
                    if (tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null || tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null)
                    {
                        throw new InvalidOperationException("OnCompare properties Config can not be set for Entity with Spatial types like 'Geometry'");
                    }
                }

                if (!columnsDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
            }
            else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
            {
                var fk = entityShadowFkPropertiesDict[property.Name];

                entityPropertiesDict.TryGetValue(fk.GetColumnName(objectIdentifier) ?? string.Empty, out var entityProperty);
                if (entityProperty == null) // BulkRead
                    continue;

                var columnName = entityProperty.GetColumnName(objectIdentifier);

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName ?? string.Empty);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName ?? string.Empty].ProviderClrType
                                                 : entityProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry) && isOracle)
                {
                    propertyType = typeof(byte[]);
                }

                if (columnName is not null && !(columnsDict.ContainsKey(columnName)) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
            }
            else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
            {
                //Type? navOwnedType = type.Assembly.GetType(property.PropertyType.FullName!); // was not used

                var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                if (ownedEntityType == null)
                {
                    ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
                }

                var ownedEntityProperties = ownedEntityType?.GetProperties().ToList() ?? new();
                var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                foreach (var ownedEntityProperty in ownedEntityProperties)
                {
                    if (!ownedEntityProperty.IsPrimaryKey())
                    {
                        string? columnName = ownedEntityProperty.GetColumnName(objectIdentifier);
                        if (columnName is not null && tableInfo.PropertyColumnNamesDict.ContainsValue(columnName))
                        {
                            ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                            ownedEntitiesMappedProperties.Add(property.Name + "_" + ownedEntityProperty.Name);
                        }
                    }
                }

                var innerProperties = property.PropertyType.GetProperties();
                if (!tableInfo.LoadOnlyPKColumn)
                {
                    foreach (var innerProperty in innerProperties)
                    {
                        if (ownedEntityPropertyNameColumnNameDict.ContainsKey(innerProperty.Name))
                        {
                            var columnName = ownedEntityPropertyNameColumnNameDict[innerProperty.Name];
                            var propertyName = $"{property.Name}_{innerProperty.Name}";

                            if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(propertyName))
                            {
                                var convertor = tableInfo.ConvertibleColumnConverterDict[propertyName];
                                var underlyingType = Nullable.GetUnderlyingType(convertor.ProviderClrType) ?? convertor.ProviderClrType;
                                dataTable.Columns.Add(columnName, underlyingType);
                            }
                            else
                            {
                                var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                dataTable.Columns.Add(columnName, ownedPropertyType);
                            }

                            columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                        }
                    }
                }
            }
        }

        if (tableInfo.BulkConfig.EnableShadowProperties)
        {
            foreach (var shadowProperty in entityPropertiesDict.Values.Where(a => a.IsShadowProperty()))
            {
                string? columnName = shadowProperty.GetColumnName(objectIdentifier);

                // If a model has an entity which has a relationship without an explicity defined FK, the data table will already contain the foreign key shadow property
                if (columnName is not null && dataTable.Columns.Contains(columnName))
                    continue;

                var isConvertible = columnName is not null && tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);

                var propertyType = isConvertible
                    ? tableInfo.ConvertibleColumnConverterDict[columnName!].ProviderClrType
                    : shadowProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isOracle && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(byte[]);
                }

                dataTable.Columns.Add(columnName, propertyType);
                columnsDict.Add(shadowProperty.Name, null);
            }
        }

        if (discriminatorColumn != null)
        {
            var discriminatorProperty = entityPropertiesDict[discriminatorColumn];

            dataTable.Columns.Add(discriminatorColumn, discriminatorProperty.ClrType);
            columnsDict.Add(discriminatorColumn, entityType.GetDiscriminatorValue());
        }
        bool hasConverterProperties = tableInfo.ConvertiblePropertyColumnDict.Count > 0;

        foreach (T entity in entities)
        {
            var propertiesToLoad = properties
                .Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name)
                            || entityShadowFkPropertiesDict.ContainsKey(a.Name)
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name)); // omit virtual Navigation (except Owned and ShadowNavig.) since it's Getter can cause unwanted Select-s from Db

            foreach (var property in propertiesToLoad)
            {
                object? propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name)
                    ? tableInfo.FastPropertyDict[property.Name].Get(entity!)
                    : null;

                var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                                           && !tableInfo.BulkConfig.SetOutputIdentity
                                           && tableInfo.DefaultValueProperties.Contains(property.Name);

                if (isOracle && tableInfo.BulkConfig.DateTime2PrecisionForceRound
                            && tableInfo.DateTime2PropertiesPrecisionLessThen7Dict.ContainsKey(property.Name))
                {
                    DateTime? dateTimePropertyValue = (DateTime?)propertyValue;

                    if (dateTimePropertyValue is not null)
                    {
                        int precision = tableInfo.DateTime2PropertiesPrecisionLessThen7Dict[property.Name];
                        int digitsToRemove = 7 - precision;
                        int powerOf10 = (int)Math.Pow(10, digitsToRemove);

                        long subsecondTicks = dateTimePropertyValue.Value!.Ticks % 10000000;
                        long ticksToRound = subsecondTicks + (subsecondTicks % 10 == 0 ? 1 : 0); // if ends with 0 add 1 tick to make sure rounding of value .5_zeros is rounded to Upper like SqlServer is doing, not to Even as Math.Round works
                        int roundedTicks = Convert.ToInt32(Math.Round((decimal)ticksToRound / powerOf10, 0)) * powerOf10;
                        dateTimePropertyValue = dateTimePropertyValue.Value!.AddTicks(-subsecondTicks).AddTicks(roundedTicks);

                        propertyValue = dateTimePropertyValue;
                    }
                }

                if (hasConverterProperties && tableInfo.ConvertiblePropertyColumnDict.ContainsKey(property.Name))
                {
                    string columnName = tableInfo.ConvertiblePropertyColumnDict[property.Name];
                    propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                }

                //TODO: Hamdling special types

                if (tableInfo.HasSpatialType && propertyValue is Geometry geometryValue)
                {
                    geometryValue.SRID = tableInfo.BulkConfig.SRID;
                    var wkb = geometryValue.ToBinary();
                    propertyValue = wkb;
                }

                if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    columnsDict[property.Name] = propertyValue;
                }
                else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                {
                    var foreignKeyShadowProperty = entityShadowFkPropertiesDict[property.Name];
                    var columnName = entityShadowFkPropertyColumnNamesDict[property.Name] ?? string.Empty;
                    if (!entityPropertiesDict.TryGetValue(columnName, out var entityProperty) || entityProperty is null)
                    {
                        continue; // BulkRead
                    };
                    columnsDict[columnName] = propertyValue != null ? foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(propertyValue) // TODO Try to optimize
                                                                    : propertyValue;
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                {
                    var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                    foreach (var ownedProperty in ownedProperties)
                    {
                        var columnName = $"{property.Name}_{ownedProperty.Name}";
                        var ownedPropertyValue = propertyValue == null ? null : tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                        if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                        {
                            var converter = tableInfo.ConvertibleColumnConverterDict[columnName];
                            columnsDict[columnName] = ownedPropertyValue == null ? null : converter.ConvertToProvider.Invoke(ownedPropertyValue);
                        }
                        else
                        {
                            columnsDict[columnName] = ownedPropertyValue;
                        }
                    }
                }
            }

            if (tableInfo.BulkConfig.EnableShadowProperties)
            {
                foreach (var shadowPropertyName in shadowPropertyColumnNamesDict.Keys)
                {
                    var shadowProperty = entityPropertiesDict[shadowPropertyName];
                    var columnName = shadowPropertyColumnNamesDict[shadowPropertyName] ?? string.Empty;

                    var propertyValue = default(object);

                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        propertyValue = context.Entry(entity!).Property(shadowPropertyName).CurrentValue;
                    }
                    else
                    {
                        propertyValue = tableInfo.BulkConfig.ShadowPropertyValue(entity!, shadowPropertyName);
                    }

                    if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                    {
                        propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                    }

                    columnsDict[shadowPropertyName] = propertyValue;
                }
            }

            var record = columnsDict.Values.ToArray();
            dataTable.Rows.Add(record);
        }

        return dataTable;
    }

    private static string? GetDiscriminatorColumn(TableInfo tableInfo)
    {
        string? discriminatorColumn = null;
        if (!tableInfo.BulkConfig.EnableShadowProperties && tableInfo.ShadowProperties.Count > 0)
        {
            var stringColumns = tableInfo.ColumnNamesTypesDict.Where(a => a.Value.Contains("char")).Select(a => a.Key).ToList();
            discriminatorColumn = tableInfo.ShadowProperties.Where(a => stringColumns.Contains(a)).ElementAt(0);
        }
        return discriminatorColumn;
    }
    #endregion
}
