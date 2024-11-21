using System.Data.Common;
using System.Globalization;
using Northwind.Services.Repositories;

namespace Northwind.Services.Ado.Repositories
{
    public sealed class OrderRepository : IOrderRepository
    {
        private readonly DbProviderFactory dbProviderFactory;
        private readonly string connectionString;

        public OrderRepository(DbProviderFactory dbFactory, string connectionString)
        {
            this.dbProviderFactory = dbFactory;
            this.connectionString = connectionString;
        }

        public async Task<long> AddOrderAsync(Order order)
        {
            try
            {
                foreach (var od in order.OrderDetails)
                {
                    if (od.Order.Id < 1 || od.Product.Id < 1 || od.UnitPrice < 0 || od.Quantity < 1 || (decimal)od.Discount < 0)
                    {
                        throw new RepositoryException("incorrect OrderDetails data");
                    }
                }

                using var connection = this.dbProviderFactory.CreateConnection();
                connection!.ConnectionString = this.connectionString;
                connection.Open();

                var queryExistOrder = $"SELECT COUNT(*) FROM Orders WHERE OrderID = @OrderID";
                var commandExistOrder = connection.CreateCommand();
                commandExistOrder.CommandText = queryExistOrder;

                commandExistOrder.AddParameterWithValue("OrderID", order.Id);
                var countInDb = await commandExistOrder.ExecuteScalarAsync();
                if (countInDb is not null)
                {
                    long existOrderInDb = (long)countInDb;
                    if (existOrderInDb > 0)
                    {
                        return order.Id;
                    }
                }

                var addressValue = order.ShippingAddress.Address.Replace("\'", "\'\'", StringComparison.Ordinal);
                var regionValue = order.ShippingAddress.Region is null ? "NULL" : order.ShippingAddress.Region;

                var queryAddOrder = $"INSERT INTO Orders VALUES("
                                   + $"@Id,"
                                   + $"@CustomerCode,"
                                   + $"@EmployeeId,"
                                   + $"@OrderDate,"
                                   + $"@RequiredDate,"
                                   + $"@ShippedDate,"
                                   + $"@ShipperId,"
                                   + $"@Freight,"
                                   + $"@ShipName,"
                                   + $"@AddressValue,"
                                   + $"@City,"
                                   + $"@RegionValue,"
                                   + $"@PostalCode,"
                                   + $"@Country);";

                var commandAddOrder = connection.CreateCommand();
                commandAddOrder.CommandText = queryAddOrder;
                commandAddOrder.AddParameterWithValue("@Id", order.Id);
                commandAddOrder.AddParameterWithValue("@CustomerCode", order.Customer.Code.Code);
                commandAddOrder.AddParameterWithValue("@EmployeeId", order.Employee.Id);
                commandAddOrder.AddParameterWithValue("@OrderDate", order.OrderDate);
                commandAddOrder.AddParameterWithValue("@RequiredDate", order.RequiredDate);
                commandAddOrder.AddParameterWithValue("@ShippedDate", order.ShippedDate);
                commandAddOrder.AddParameterWithValue("@ShipperId", order.Shipper.Id);
                commandAddOrder.AddParameterWithValue("@Freight", order.Freight.ToString(new CultureInfo("en-US")).Replace(",", ".", StringComparison.Ordinal));
                commandAddOrder.AddParameterWithValue("@ShipName", order.ShipName);
                commandAddOrder.AddParameterWithValue("@AddressValue", addressValue);
                commandAddOrder.AddParameterWithValue("@City", order.ShippingAddress.City);
                commandAddOrder.AddParameterWithValue("@RegionValue", regionValue);
                commandAddOrder.AddParameterWithValue("@PostalCode", order.ShippingAddress.PostalCode);
                commandAddOrder.AddParameterWithValue("@Country", order.ShippingAddress.Country);
                var result = await commandAddOrder.ExecuteNonQueryAsync();
                foreach (var od in order.OrderDetails)
                {
                    var queryAddOrderDetail = $"INSERT INTO OrderDetails VALUES(@OrderID, @ProductID, @UnitPrice, @Quantity, @Discount);";
                    var commandAddOrderDetail = connection.CreateCommand();
                    commandAddOrderDetail.CommandText = queryAddOrderDetail;
                    commandAddOrderDetail.AddParameterWithValue("@OrderID", od.Order.Id);
                    commandAddOrderDetail.AddParameterWithValue("@ProductID", od.Product.Id);
                    commandAddOrderDetail.AddParameterWithValue("@UnitPrice", (decimal)od.UnitPrice);
                    commandAddOrderDetail.AddParameterWithValue("@Quantity", od.Quantity);
                    commandAddOrderDetail.AddParameterWithValue("@Discount", (decimal)od.Discount);
                    _ = await commandAddOrderDetail.ExecuteNonQueryAsync();
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new RepositoryException(ex.Message);
            }
        }

        public async Task<Order> GetOrderAsync(long orderId)
        {
            using var connection = this.dbProviderFactory.CreateConnection();
            connection!.ConnectionString = this.connectionString;
            connection.Open();
            var order = await GetOrderById(orderId, connection);
            return order;
        }

        public async Task<IList<Order>> GetOrdersAsync(int skip, int count)
        {
            CheckRangeInput(skip, count);

            IList<Order> orders = new List<Order>();
            using var connection = this.dbProviderFactory.CreateConnection();
            connection!.ConnectionString = this.connectionString;
            connection.Open();
            var selectOrdersSql = $"SELECT * FROM Orders LIMIT @Count OFFSET @Skip;";
            var command = connection.CreateCommand();
            command.CommandText = selectOrdersSql;
            command.AddParameterWithValue("@Count", count);
            command.AddParameterWithValue("@Skip", skip);
            var reader = await command.ExecuteReaderAsync();
            while (reader.Read())
            {
                var orderId = reader.GetInt64(0);
                var currentOrder = await GetOrderById(orderId, connection);
                orders.Add(currentOrder);
            }

            return orders;
        }

        public async Task RemoveOrderAsync(long orderId)
        {
            var queryRemoveOrder = $"DELETE FROM Orders WHERE OrderID = @OrderID";
            var queryRemoveOrderDetails = @"DELETE FROM OrderDetails WHERE OrderID = @OrderID";
            using var connection = this.dbProviderFactory.CreateConnection();
            using var commandRemoveOrder = connection!.CreateCommand();
            using var commandRemoveOrderDetails = connection!.CreateCommand();
            connection!.ConnectionString = this.connectionString;
            connection.Open();
            commandRemoveOrder.CommandText = queryRemoveOrder;
            commandRemoveOrderDetails.CommandText = queryRemoveOrderDetails;
            commandRemoveOrder.AddParameterWithValue("@OrderID", orderId);
            commandRemoveOrderDetails.AddParameterWithValue("@OrderID", orderId);
            _ = await commandRemoveOrderDetails.ExecuteNonQueryAsync();
            _ = await commandRemoveOrder.ExecuteNonQueryAsync();
        }

        public async Task UpdateOrderAsync(Order order)
        {
            try
            {
                var queryDeleteOrderDetails = "DELETE FROM OrderDetails WHERE OrderID = @OrderID";
                using var connection = this.dbProviderFactory.CreateConnection();
                connection!.ConnectionString = this.connectionString;
                connection.Open();
                var cmdDeleteOrderDetails = connection.CreateCommand();
                cmdDeleteOrderDetails.CommandText = queryDeleteOrderDetails;
                cmdDeleteOrderDetails.AddParameterWithValue("@OrderID", order.Id);
                _ = await cmdDeleteOrderDetails.ExecuteNonQueryAsync();

                foreach (var od in order.OrderDetails)
                {
                    var insertQuery = $"INSERT INTO OrderDetails VALUES(" +
                                      $"@OrderID," +
                                      $"@ProductID," +
                                      $"@UnitPrice," +
                                      $"@Quantity," +
                                      $"@Discount);";

                    var cmdInsertOrderDetails = connection.CreateCommand();
                    cmdInsertOrderDetails.CommandText = insertQuery;
                    cmdInsertOrderDetails.AddParameterWithValue("@OrderID", od.Order.Id);
                    cmdInsertOrderDetails.AddParameterWithValue("@ProductID", od.Product.Id);
                    cmdInsertOrderDetails.AddParameterWithValue("@UnitPrice", od.UnitPrice);
                    cmdInsertOrderDetails.AddParameterWithValue("@Quantity", od.Quantity);
                    cmdInsertOrderDetails.AddParameterWithValue("@Discount", od.Discount);
                    _ = await cmdInsertOrderDetails.ExecuteNonQueryAsync();
                }

                var queryUpdateOrder = $"UPDATE Orders SET " +
                                        $"OrderID = @OrderID," +
                                        $"CustomerID = @CustomerID," +
                                        $"EmployeeID = @EmployeeID," +
                                        $"OrderDate = @OrderDate," +
                                        $"RequiredDate = @RequiredDate," +
                                        $"ShippedDate = @ShippedDate," +
                                        $"ShipVia = @ShipVia," +
                                        $"Freight = @Freight," +
                                        $"ShipName = @ShipName," +
                                        $"ShipAddress = @ShipAddress," +
                                        $"ShipCity = @ShipCity," +
                                        $"ShipRegion = @ShipRegion," +
                                        $"ShipPostalCode = @ShipPostalCode," +
                                        $"ShipCountry = @ShipCountry " +
                                        $"WHERE OrderID = @OrderID;";

                var cmdUpdateOrder = connection.CreateCommand();
                cmdUpdateOrder.CommandText = queryUpdateOrder;
                cmdUpdateOrder.AddParameterWithValue("@OrderID", order.Id);
                cmdUpdateOrder.AddParameterWithValue("@CustomerID", order.Customer.Code.Code);
                cmdUpdateOrder.AddParameterWithValue("@EmployeeID", order.Employee.Id);

                cmdUpdateOrder.AddParameterWithValue("@OrderDate", order.OrderDate.ToString("MM/dd/yyyy", CultureInfo.InvariantCulture));
                cmdUpdateOrder.AddParameterWithValue("@RequiredDate", order.RequiredDate.ToString("MM/dd/yyyy", CultureInfo.InvariantCulture));
                cmdUpdateOrder.AddParameterWithValue("@ShippedDate", order.ShippedDate.ToString("MM/dd/yyyy", CultureInfo.InvariantCulture));

                cmdUpdateOrder.AddParameterWithValue("@ShipVia", order.Shipper.Id);
                cmdUpdateOrder.AddParameterWithValue("@Freight", order.Freight);
                cmdUpdateOrder.AddParameterWithValue("@ShipName", order.ShipName);
                cmdUpdateOrder.AddParameterWithValue("@ShipAddress", order.ShippingAddress.Address);
                cmdUpdateOrder.AddParameterWithValue("@ShipCity", order.ShippingAddress.City);
                cmdUpdateOrder.AddParameterWithValue("@ShipRegion", order.ShippingAddress.Region!);
                cmdUpdateOrder.AddParameterWithValue("@ShipPostalCode", order.ShippingAddress.PostalCode);
                cmdUpdateOrder.AddParameterWithValue("@ShipCountry", order.ShippingAddress.Country);
                _ = await cmdUpdateOrder.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                throw new RepositoryException(ex.Message);
            }
        }

        private static void CheckRangeInput(int skip, int count)
        {
            if (count <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count), "invalid count: out of range");
            }

            if (skip < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(skip), "invalid skip: out of range");
            }
        }

        private static async Task<Order> GetOrderById(long orderId, DbConnection connection)
        {
            var selectOrdersSql = $"SELECT * FROM Orders WHERE OrderID = @OrderID;";
            var commandAddOrder = connection.CreateCommand();
            commandAddOrder.CommandText = selectOrdersSql;
            commandAddOrder.AddParameterWithValue("@OrderID", orderId);
            using var currentOrder = await commandAddOrder.ExecuteReaderAsync();
            var isOrderExist = currentOrder.Read();
            if (!isOrderExist)
            {
                throw new RepositoryException();
            }

            long orderID = currentOrder.GetInt64(0);
            string customerID = currentOrder.GetString(1);
            long employeeID = currentOrder.GetInt64(2);
            var orderDateValue = currentOrder.GetString(3);
            DateTime orderDate = DateTime.ParseExact(orderDateValue, "M/d/yyyy", CultureInfo.CreateSpecificCulture("en-US"));
            DateTime requiredDate = DateTime.ParseExact((string)currentOrder.GetValue(4), "M/d/yyyy", CultureInfo.CreateSpecificCulture("en-US"));
            DateTime shippedDate = DateTime.ParseExact((string)currentOrder.GetValue(5), "M/d/yyyy", CultureInfo.CreateSpecificCulture("en-US"));
            long shipVia = currentOrder.GetInt64(6);
            double freight = currentOrder.GetDouble(7);
            string shipName = currentOrder.GetString(8);
            string shipAddress = currentOrder.GetString(9);
            string shipCity = currentOrder.GetString(10);
            object? regionValue = currentOrder.GetValue(11);
            string? shipRegion = regionValue is null ? null : regionValue.ToString();
            string shipPostalCode = currentOrder.GetString(12);
            string shipCountry = currentOrder.GetString(13);
            var resultOrder = new Order(orderID)
            {
                Customer = await GetCustomerByIdAsync(customerID, connection),
                Employee = await GetEmployeeByIdAsync(employeeID, connection),
                OrderDate = orderDate,
                RequiredDate = requiredDate,
                ShippedDate = shippedDate,
                Shipper = await GetShipperByID(shipVia, connection),
                Freight = freight,
                ShipName = shipName,
                ShippingAddress = new ShippingAddress(shipAddress, shipCity, shipRegion == string.Empty ? null : shipRegion, shipPostalCode, shipCountry),
            };

            var list = await GetOrderDetails(resultOrder, connection);
            foreach (var od in list)
            {
                resultOrder.OrderDetails.Add(od);
            }

            return resultOrder;
        }

        private static async Task<Customer> GetCustomerByIdAsync(string customerID, DbConnection connection)
        {
            var selectCustomerSql = $"SELECT * FROM Customers WHERE CustomerID = @customerID";
            var command = connection.CreateCommand();
            command.CommandText = selectCustomerSql;
            command.AddParameterWithValue("customerID", customerID);
            using var reader = await command.ExecuteReaderAsync();
            _ = reader.Read();
            var companyName = reader.GetString(1);
            Customer customer = new Customer(new CustomerCode(customerID)) { CompanyName = companyName };
            return customer;
        }

        private static async Task<Employee> GetEmployeeByIdAsync(long employeeID, DbConnection connection)
        {
            var selectEmployeeSql = $"SELECT * FROM Employees WHERE EmployeeID = @EmployeeID;";
            var command = connection.CreateCommand();
            command.CommandText = selectEmployeeSql;
            command.AddParameterWithValue("@EmployeeID", employeeID);
            using var reader = await command.ExecuteReaderAsync();
            _ = reader.Read();
            var employeeLastName = reader.GetString(1);
            var employeeFirstName = reader.GetString(2);
            var employeeCountry = reader.GetString(11);
            return new Employee(employeeID) { LastName = employeeLastName, FirstName = employeeFirstName, Country = employeeCountry };
        }

        private static async Task<Shipper> GetShipperByID(long shipperID, DbConnection connection)
        {
            var queryShippersSql = $"SELECT * FROM Shippers WHERE ShipperID = @ShipperID;";
            var command = connection.CreateCommand();
            command.CommandText = queryShippersSql;
            command.AddParameterWithValue("@ShipperID", shipperID);
            using var reader = await command.ExecuteReaderAsync();
            _ = reader.Read();
            var shipperCompanyName = reader.GetString(1);
            return new Shipper(shipperID) { CompanyName = shipperCompanyName };
        }

        private static async Task<IEnumerable<OrderDetail>> GetOrderDetails(Order order, DbConnection connection)
        {
            List<OrderDetail> result = new List<OrderDetail>();
            var queryOrderDetails = $"SELECT * FROM OrderDetails WHERE OrderID = @OrderID;";
            var command = connection.CreateCommand();
            command.CommandText = queryOrderDetails;
            command.AddParameterWithValue("@OrderID", order.Id);
            using var reader = await command.ExecuteReaderAsync();
            while (reader.Read())
            {
                long productID = reader.GetInt64(1);
                double unitPrice = reader.GetDouble(2);
                long quantity = reader.GetInt64(3);
                double discount = reader.GetDouble(4);
                OrderDetail orderDetail = new OrderDetail(order)
                {
                    Product = await GetProductByID(productID, connection),
                    UnitPrice = unitPrice,
                    Quantity = quantity,
                    Discount = discount,
                };
                result.Add(orderDetail);
            }

            return result;
        }

        private static async Task<Product> GetProductByID(long productID, DbConnection connection)
        {
            var queryGetProduct = $"SELECT * FROM Products WHERE ProductID = @ProductID;";
            var command = connection.CreateCommand();
            command.CommandText = queryGetProduct;
            command.AddParameterWithValue("@ProductID", productID);
            using var reader = await command.ExecuteReaderAsync();
            _ = reader.Read();
            string productName = reader.GetString(1);
            long supplierID = reader.GetInt64(2);
            long categoryID = reader.GetInt64(3);

            var queryGetCategory = $"SELECT * FROM Categories WHERE CategoryID = @CategoryID;";
            var commandCategory = connection.CreateCommand();
            commandCategory.CommandText = queryGetCategory;
            commandCategory.AddParameterWithValue("@CategoryID", categoryID);
            using var queryReader = await commandCategory.ExecuteReaderAsync();
            _ = queryReader.Read();
            string categoryName = queryReader.GetString(1);

            var queryGetSuppliers = $"SELECT * FROM Suppliers WHERE SupplierID = @SupplierID;";
            var commandSuppliers = connection.CreateCommand();
            commandSuppliers.CommandText = queryGetSuppliers;
            commandSuppliers.AddParameterWithValue("@SupplierID", supplierID);
            using var querySuppliersReader = await commandSuppliers.ExecuteReaderAsync();
            _ = querySuppliersReader.Read();
            string supplierName = querySuppliersReader.GetString(1);

            return new Product(productID) { ProductName = productName, SupplierId = supplierID, CategoryId = categoryID, Category = categoryName, Supplier = supplierName };
        }
    }
}
