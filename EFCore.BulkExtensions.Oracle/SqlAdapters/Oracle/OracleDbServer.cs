using Microsoft.EntityFrameworkCore.Infrastructure;
using Oracle.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.Oracle;

/// <inheritdoc/>

public class OracleDbServer : IDbServer
{
    SqlType IDbServer.Type => SqlType.MySql;

    OracleAdapter _adapter = new();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    OracleDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    SqlAdapters.SqlQueryBuilder _queryBuilder = new OracleQueryBuilder();
    /// <inheritdoc/>
    public SqlQueryBuilder QueryBuilder => _queryBuilder;

    string IDbServer.ValueGenerationStrategy => nameof(OracleValueGenerationStrategy);

    /// <inheritdoc/>
    public DbConnection? DbConnection { get; set; }

    /// <inheritdoc/>
    public DbTransaction? DbTransaction { get; set; }

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (OracleValueGenerationStrategy?)annotation.Value == OracleValueGenerationStrategy.IdentityColumn;
}
