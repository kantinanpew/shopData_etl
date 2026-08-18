# ShopData ETL

Technical assignment for the Data Engineer role. Reads the raw views in
`shopdata.db`

## Data exploration

What I found:

- **Duplicate customers** – customer 1 and 2 show up twice with different
  signup dates and emails. Keeping the row with the latest signup_date as
  the spec says.
- **Missing contact info** – 2 null emails, 2 null phones (customer 8 has
  neither). Emails get `unknown@domain.com`, phones stay null.
- **Phone formats are all over the place** – 8 of 12 rows have spaces,
  dashes, brackets or letters (`Ext 444`, `1-800-555-DINO`). Stripping to
  digits fixes most of them, but those two end up as `444` and `1800555`
  which aren't real numbers. Worth flagging to the business.
- **Bad amounts** – 3 orders with amount <= 0. Two are tagged SYSTEM_ERROR,
  one is a 0.0 marked COMPLETED, so filtering on status alone wouldn't catch it.
- **Missing currency / date** – 2 orders with null currency, 1 with null
  order_date.
- **Orphan orders** – orders 106 and 118 belong to customer_id 99 which
  doesn't exist. Keeping them in fct_orders but they won't show in the CLV
  report since that joins on dim_customers.
- **Exchange rate gap** – rates only cover 2023-05-01 to 05-05 but orders go
  to 05-14. 5 non-USD orders have no rate and get treated as USD per the
  spec. Order 115 (25000 JPY) becomes $25000 because of this and ends up
  top of the CLV ranking. In a real pipeline I'd forward-fill the last known
  rate; for now these rows are flagged in the output.
- **Non-completed orders** – one CANCELLED and one PENDING with positive
  amounts. Spec only filters on amount so they stay, and `status` is kept in
  fct_orders so BI can filter later.
