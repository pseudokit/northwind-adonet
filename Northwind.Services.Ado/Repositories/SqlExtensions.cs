using System.Data.Common;

namespace Northwind.Services.Ado.Repositories
{
    public static class SqlExtensions
    {
        public static void AddParameterWithValue(this DbCommand command, string parameterName, object parameterValue)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = parameterName;
            parameter.Value = parameterValue;
            _ = command.Parameters.Add(parameter);
        }
    }
}
