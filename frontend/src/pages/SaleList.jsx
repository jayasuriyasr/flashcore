import { useEffect, useState } from "react";
import { listSales } from "../api.js";

export default function SaleList() {
  const [sales, setSales] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    listSales().then(setSales).catch(() => setError("Could not load sales."));
  }, []);

  if (error) return <p className="error">{error}</p>;
  if (!sales) return <p className="muted">Loading sales…</p>;
  if (sales.length === 0) return <p className="muted">No sales yet. Create one in the admin.</p>;

  return (
    <div className="grid">
      {sales.map((sale) => (
        <a key={sale.id} className="card sale-card" href={`#/sales/${sale.id}`}>
          <h2>{sale.product_name}</h2>
          <p className="price">₹{sale.price}</p>
          <p className="muted">
            {sale.remaining} of {sale.total_stock} left · starts {new Date(sale.starts_at).toLocaleString()}
          </p>
        </a>
      ))}
    </div>
  );
}
