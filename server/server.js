const express = require('express');
const mongoose = require('mongoose');

const app = express();
const port = process.env.PORT || 3000;

const cors = require('cors');

app.use(cors())


// MongoDB connection string
const dbURI = "mongodb+srv://db_user_read:LdmrVA5EDEv4z3Wr@cluster0.n10ox.mongodb.net/RQ_Analytics?retryWrites=true&w=majority";

mongoose.connect(dbURI, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB'))
  .catch(err => console.log(err));

app.use(express.json());

// Add the API route
app.get('/', (req, res) => {
  res.send('Welcome to EcomAnalytics API');
});
app.get('/api/sales-over-time', async (req, res) => {
  const { interval } = req.query; // daily, monthly, quarterly, yearly

  let groupBy;
  switch (interval) {
    case 'daily':
      groupBy = { $dateToString: { format: "%Y-%m-%d", date: { $toDate: "$created_at" } } };
      break;
    case 'monthly':
      groupBy = { $dateToString: { format: "%Y-%m", date: { $toDate: "$created_at" } } };
      break;
    case 'quarterly':
      groupBy = { $dateToString: { format: "%Y-Q", date: { $toDate: "$created_at" } } };
      break;
    case 'yearly':
      groupBy = { $dateToString: { format: "%Y", date: { $toDate: "$created_at" } } };
      break;
    default:
      return res.status(400).json({ message: 'Invalid interval' });
  }

  try {
    const db = mongoose.connection.db;  // Access the current MongoDB connection
    const ordersCollection = db.collection('shopifyOrders');  // Get the 'shopifyOrders' collection

    const sales = await ordersCollection.aggregate([
      {
        $group: {
          _id: groupBy,
          total_sales: { $sum: { $toDouble: "$total_price_set.shop_money.amount" } }  // Convert the total_price to a double for summing
        }
      },
      { $sort: { _id: 1 } }  // Sort the results by the time interval
    ]).toArray();  // Convert the aggregation result to an array

    res.json(sales);
  } catch (error) {
    console.error('Error during aggregation:', error); // Debugging output
    res.status(500).json({ message: error.message });
  }
});

//Growth rate
app.get('/api/sales-growth-rate', async (req, res) => {
    const { interval } = req.query; // daily, monthly, quarterly, yearly
  
    let groupBy;
    switch (interval) {
      case 'daily':
        groupBy = { $dateToString: { format: "%Y-%m-%d", date: { $toDate: "$created_at" } } };
        break;
      case 'monthly':
        groupBy = { $dateToString: { format: "%Y-%m", date: { $toDate: "$created_at" } } };
        break;
      case 'quarterly':
        groupBy = { $dateToString: { format: "%Y-Q", date: { $toDate: "$created_at" } } };
        break;
      case 'yearly':
        groupBy = { $dateToString: { format: "%Y", date: { $toDate: "$created_at" } } };
        break;
      default:
        return res.status(400).json({ message: 'Invalid interval' });
    }
  
    try {
      const db = mongoose.connection.db;
      const ordersCollection = db.collection('shopifyOrders');
  
      // Calculate total sales for each time interval
      const sales = await ordersCollection.aggregate([
        {
          $group: {
            _id: groupBy,
            total_sales: { $sum: { $toDouble: "$total_price_set.shop_money.amount" } }
          }
        },
        { $sort: { _id: 1 } }  // Sort by the time interval
      ]).toArray();
  
      // Calculate growth rate
      const salesGrowthRate = sales.map((current, index, array) => {
        if (index === 0) {
          return { period: current._id, growth_rate: null };  // No previous period to compare
        }
        const previous = array[index - 1];
        const growthRate = ((current.total_sales - previous.total_sales) / previous.total_sales) * 100;
        return { period: current._id, growth_rate: growthRate };
      });
  
      res.json(salesGrowthRate);
    } catch (error) {
      console.error('Error during aggregation:', error);
      res.status(500).json({ message: error.message });
    }
  });
//New Customers added
app.get('/api/new-customers-over-time', async (req, res) => {
    const { interval } = req.query; // daily, monthly, quarterly, yearly
  
    let groupBy;
    switch (interval) {
      case 'daily':
        groupBy = { $dateToString: { format: "%Y-%m-%d", date: { $toDate: "$created_at" } } };
        break;
      case 'monthly':
        groupBy = { $dateToString: { format: "%Y-%m", date: { $toDate: "$created_at" } } };
        break;
      case 'quarterly':
        groupBy = { $dateToString: { format: "%Y-Q", date: { $toDate: "$created_at" } } };
        break;
      case 'yearly':
        groupBy = { $dateToString: { format: "%Y", date: { $toDate: "$created_at" } } };
        break;
      default:
        return res.status(400).json({ message: 'Invalid interval' });
    }
  
    try {
      const db = mongoose.connection.db;
      const customersCollection = db.collection('shopifyCustomers');
  
      // Aggregate new customers by the specified time interval
      const newCustomers = await customersCollection.aggregate([
        {
          $group: {
            _id: groupBy,
            count: { $sum: 1 }
          }
        },
        { $sort: { _id: 1 } }  // Sort by the time interval
      ]).toArray();
  
      res.json(newCustomers);
    } catch (error) {
      console.error('Error during aggregation:', error);
      res.status(500).json({ message: error.message });
    }
  });
  //Repeat Customers
  app.get('/api/repeat-customers', async (req, res) => {
    const { interval } = req.query; // daily, monthly, quarterly, yearly
  
    let groupBy;
    switch (interval) {
      case 'daily':
        groupBy = { $dateToString: { format: "%Y-%m-%d", date: { $toDate: "$created_at" } } };
        break;
      case 'monthly':
        groupBy = { $dateToString: { format: "%Y-%m", date: { $toDate: "$created_at" } } };
        break;
      case 'quarterly':
        groupBy = { $dateToString: { format: "%Y-Q", date: { $toDate: "$created_at" } } };
        break;
      case 'yearly':
        groupBy = { $dateToString: { format: "%Y", date: { $toDate: "$created_at" } } };
        break;
      default:
        return res.status(400).json({ message: 'Invalid interval' });
    }
  
    try {
      const db = mongoose.connection.db;
      const ordersCollection = db.collection('shopifyOrders');
  
      // Aggregate to find customers with more than one purchase
      const repeatCustomers = await ordersCollection.aggregate([
        {
          $group: {
            _id: {
              customer_id: "$customer.id",
              period: groupBy
            },
            purchase_count: { $sum: 1 }
          }
        },
        {
          $match: {
            "purchase_count": { $gt: 1 }  // Filter to only include repeat customers
          }
        },
        {
          $group: {
            _id: "$_id.period",
            repeat_customers: { $sum: 1 }
          }
        },
        { $sort: { _id: 1 } }  // Sort by the time interval
      ]).toArray();
  
      res.json(repeatCustomers);
    } catch (error) {
      console.error('Error during aggregation:', error);
      res.status(500).json({ message: error.message });
    }
  });

  //geographical distribution
  app.get('/api/customer-geography', async (req, res) => {
    try {
      const db = mongoose.connection.db;
      const customersCollection = db.collection('shopifyCustomers');
  
      // Aggregate to count customers by city
      const geographicalDistribution = await customersCollection.aggregate([
        {
          $group: {
            _id: "$default_address.city",
            customer_count: { $sum: 1 }
          }
        },
        { $sort: { customer_count: -1 } }  // Sort by customer count in descending order
      ]).toArray();
  
      res.json(geographicalDistribution);
    } catch (error) {
      console.error('Error during aggregation:', error);
      res.status(500).json({ message: error.message });
    }
  });

  //Lifetime value by Cohorts
  app.get('/api/clv-by-cohorts', async (req, res) => {
    try {
      const db = mongoose.connection.db;
      const ordersCollection = db.collection('shopifyOrders');
  
      const customers = await ordersCollection.aggregate([
        // Match all documents
        { $match: { total_price: { $exists: true } } },
  
        // Convert string to date
        {
          $addFields: {
            created_at: { $dateFromString: { dateString: "$created_at" } }
          }
        },
  
        // Group by customer ID to sum up total revenue for each customer
        {
          $group: {
            _id: "$customer.id",
            total_revenue: { $sum: { $toDouble: "$total_price" } },
            first_order_date: { $min: "$created_at" }  // Track the first order date
          }
        },
  
        // Add a field for cohort month based on the first order date
        {
          $addFields: {
            cohort_month: {
              $dateToString: { format: "%Y-%m", date: "$first_order_date" }
            }
          }
        },
  
        // Group by cohort month to sum up total revenue for each cohort
        {
          $group: {
            _id: "$cohort_month",
            total_revenue: { $sum: "$total_revenue" },
            customer_count: { $sum: 1 }
          }
        },
  
        // Calculate average revenue per customer for each cohort
        {
          $addFields: {
            average_clv: {
              $divide: ["$total_revenue", "$customer_count"]
            }
          }
        }
      ]).toArray();
  
      res.json(customers);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  });
  
  
  app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({ error: 'Internal Server Error' });
  });

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
