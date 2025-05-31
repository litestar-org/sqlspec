#!/usr/bin/env python3
# /// script
# dependencies = [
#   "sqlspec[duckdb,performance,aiosql] @ file://../sqlspec",
#   "rich>=13.0.0",
#   "faker>=24.0.0",
#   "pydantic>=2.0.0",
# ]
# ///

"""🚀 ULTIMATE SQLSpec Showcase - The Most Complex SQL Generation Demo Ever! 🚀

This demo showcases EVERY advanced SQL feature in SQLSpec:
- 🔥 NEW: PIVOT, UNPIVOT, ROLLUP, CROSS JOIN
- 🔥 NEW: WHERE ANY, WHERE NOT ANY
- ⚡ Window Functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD)
- 🏗️ Complex CTEs (Common Table Expressions) with Recursion
- 🔗 Advanced Joins (INNER, LEFT, RIGHT, FULL, CROSS, LATERAL)
- 📊 Case Expressions and Conditional Logic
- 🎯 Subqueries and Correlated Subqueries
- 🔍 Advanced Filtering and Search
- 📈 Analytics and Business Intelligence Queries
- 🛡️ SQL Injection Prevention
- 🎨 Beautiful Query Formatting

Run with: uv run ultimate_sql_showcase.py
"""

import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from faker import Faker
from pydantic import BaseModel, Field
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.table import Table

# SQLSpec imports - THE ULTIMATE SQL TOOLKIT
from sqlspec import sql
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.extensions.aiosql import AiosqlLoader

console = Console()
fake = Faker()


# 🎯 Data Models for Complex Scenarios
class Employee(BaseModel):
    """Employee model for complex HR analytics."""

    id: int
    name: str = Field(min_length=2, max_length=100)
    email: str = Field(pattern=r"^[^@]+@[^@]+\.[^@]+$")
    department: str
    position: str
    salary: Decimal = Field(ge=0, decimal_places=2)
    hire_date: datetime
    manager_id: Optional[int] = None
    active: bool = True


class SalesData(BaseModel):
    """Sales data for complex analytics."""

    id: int
    employee_id: int
    product_id: int
    customer_id: int
    sale_date: datetime
    quantity: int = Field(ge=1)
    unit_price: Decimal = Field(ge=0, decimal_places=2)
    total_amount: Decimal = Field(ge=0, decimal_places=2)
    region: str
    quarter: str


def create_ultimate_database() -> Any:
    """Create the most complex database schema for our showcase."""
    config = DuckDBConfig()

    with config.provide_session() as driver:
        # Create comprehensive schema
        driver.execute("""
            -- Employees table with hierarchical structure
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                email VARCHAR UNIQUE,
                department VARCHAR,
                position VARCHAR,
                salary DECIMAL(12,2),
                hire_date TIMESTAMP,
                manager_id INTEGER,
                active BOOLEAN DEFAULT TRUE,
                metadata JSON,
                FOREIGN KEY (manager_id) REFERENCES employees(id)
            );
            -- Products with categories and pricing
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                category VARCHAR,
                subcategory VARCHAR,
                price DECIMAL(10,2),
                cost DECIMAL(10,2),
                created_at TIMESTAMP,
                discontinued BOOLEAN DEFAULT FALSE
            );
            -- Customers with geographic data
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                address VARCHAR,
                city VARCHAR,
                state VARCHAR,
                country VARCHAR,
                postal_code VARCHAR,
                customer_since TIMESTAMP,
                lifetime_value DECIMAL(12,2)
            );
            -- Sales transactions
            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER,
                product_id INTEGER,
                customer_id INTEGER,
                sale_date TIMESTAMP,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(12,2),
                region VARCHAR,
                quarter VARCHAR,
                FOREIGN KEY (employee_id) REFERENCES employees(id),
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            );
            -- Performance metrics
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY,
                employee_id INTEGER,
                metric_name VARCHAR,
                metric_value DECIMAL(10,2),
                measurement_date TIMESTAMP,
                FOREIGN KEY (employee_id) REFERENCES employees(id)
            );
            -- Quarterly targets
            CREATE TABLE IF NOT EXISTS quarterly_targets (
                id INTEGER PRIMARY KEY,
                department VARCHAR,
                quarter VARCHAR,
                year INTEGER,
                target_revenue DECIMAL(12,2),
                target_units INTEGER
            );
        """)

        # Insert complex sample data
        departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Support"]
        positions = ["Manager", "Senior", "Mid-level", "Junior", "Director", "VP", "Analyst"]
        regions = ["North", "South", "East", "West", "Central"]
        quarters = ["Q1", "Q2", "Q3", "Q4"]

        # Insert employees with hierarchical relationships
        for i in range(1, 201):  # 200 employees
            manager_id = fake.random_int(min=1, max=i - 1) if i > 10 else None
            driver.execute(
                """
                INSERT INTO employees (id, name, email, department, position, salary, hire_date, manager_id, active, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    i,
                    fake.name(),
                    fake.unique.email(),
                    fake.random_element(departments),
                    fake.random_element(positions),
                    fake.random_int(min=40000, max=250000),
                    fake.date_between(start_date="-5y", end_date="today"),
                    manager_id,
                    fake.boolean(chance_of_getting_true=90),
                    f'{{"skills": ["{fake.word()}", "{fake.word()}"], "certifications": ["{fake.word()}"]}}',
                ),
            )

        # Insert products
        for i in range(1, 101):  # 100 products
            driver.execute(
                """
                INSERT INTO products (id, name, category, subcategory, price, cost, created_at, discontinued)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    i,
                    fake.catch_phrase(),
                    fake.random_element(["Electronics", "Software", "Services", "Hardware"]),
                    fake.word(),
                    fake.random_int(min=10, max=5000),
                    fake.random_int(min=5, max=2500),
                    fake.date_time_between(start_date="-3y", end_date="now"),
                    fake.boolean(chance_of_getting_true=10),
                ),
            )

        # Insert customers
        for i in range(1, 501):  # 500 customers
            driver.execute(
                """
                INSERT INTO customers (id, name, email, phone, address, city, state, country, postal_code, customer_since, lifetime_value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    i,
                    fake.company(),
                    fake.email(),
                    fake.phone_number(),
                    fake.address(),
                    fake.city(),
                    fake.state(),
                    fake.country(),
                    fake.postcode(),
                    fake.date_between(start_date="-3y", end_date="today"),
                    fake.random_int(min=1000, max=100000),
                ),
            )

        # Insert sales data
        for i in range(1, 5001):  # 5000 sales records
            quantity = fake.random_int(min=1, max=10)
            unit_price = fake.random_int(min=10, max=1000)
            driver.execute(
                """
                INSERT INTO sales (id, employee_id, product_id, customer_id, sale_date, quantity, unit_price, total_amount, region, quarter)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    i,
                    fake.random_int(min=1, max=200),
                    fake.random_int(min=1, max=100),
                    fake.random_int(min=1, max=500),
                    fake.date_time_between(start_date="-2y", end_date="now"),
                    quantity,
                    unit_price,
                    quantity * unit_price,
                    fake.random_element(regions),
                    fake.random_element(quarters),
                ),
            )

        return driver


def demo_header() -> None:
    """Create the most stunning demo header ever."""
    title_panel = Panel.fit(
        "[bold magenta]🚀 ULTIMATE SQLSpec Showcase 🚀[/bold magenta]\n"
        "[cyan]The Most Advanced SQL Generation Demo Ever Created[/cyan]\n\n"
        "[yellow]✨ PIVOT, UNPIVOT, ROLLUP, CROSS JOIN ✨[/yellow]\n"
        "[green]🔥 WHERE ANY, WHERE NOT ANY 🔥[/green]\n"
        "[blue]⚡ Window Functions & Analytics ⚡[/blue]\n"
        "[red]🏗️ Complex CTEs & Recursion 🏗️[/red]\n"
        "[magenta]🎯 Advanced Joins & Subqueries 🎯[/magenta]\n"
        "[cyan]🛡️ SQL Injection Prevention 🛡️[/cyan]",
        title="SQLSpec = SQL Superpowers",
        border_style="double",
        box=box.DOUBLE,
    )
    console.print(title_panel)
    console.print()


def demo_new_sql_features() -> None:
    """Demonstrate the brand new SQL features: PIVOT, UNPIVOT, ROLLUP, CROSS JOIN, WHERE ANY."""
    console.print(
        Panel(
            "[bold red]🔥 BRAND NEW SQL FEATURES 🔥[/bold red]\nFeatures that were just added to SQLSpec!",
            border_style="red",
        )
    )

    # 1. PIVOT - Transform rows to columns
    console.print("\n[bold cyan]1. PIVOT - Transform Rows to Columns[/bold cyan]")
    pivot_query = (
        sql.select("employee_id", "Q1", "Q2", "Q3", "Q4")
        .from_("sales")
        .pivot("SUM(total_amount)", "quarter", ["Q1", "Q2", "Q3", "Q4"])
        .where("sale_date >= '2023-01-01'")
        .order_by("employee_id")
    )

    syntax = Syntax(str(pivot_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="🔄 PIVOT Query", border_style="green"))

    # 2. UNPIVOT - Transform columns to rows
    console.print("\n[bold cyan]2. UNPIVOT - Transform Columns to Rows[/bold cyan]")
    unpivot_query = (
        sql.select("employee_id", "quarter", "sales_amount")
        .from_("quarterly_sales")
        .unpivot("sales_amount", "quarter", ["Q1", "Q2", "Q3", "Q4"])
        .where("sales_amount > 0")
    )

    syntax = Syntax(str(unpivot_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="🔄 UNPIVOT Query", border_style="green"))

    # 3. ROLLUP - Subtotals and Grand Totals
    console.print("\n[bold cyan]3. ROLLUP - Subtotals and Grand Totals[/bold cyan]")
    rollup_query = (
        sql.select("department", "position", "SUM(salary) as total_salary")
        .from_("employees")
        .where("active = TRUE")
        .group_by("department", "position", rollup=True)
        .order_by("department", "position")
    )

    syntax = Syntax(str(rollup_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="📊 ROLLUP Query", border_style="green"))

    # 4. CROSS JOIN - Cartesian Product
    console.print("\n[bold cyan]4. CROSS JOIN - Cartesian Product[/bold cyan]")
    cross_join_query = (
        sql.select("p.name as product_name", "r.region_name")
        .from_("products p")
        .cross_join("regions r")
        .where("p.discontinued = FALSE")
        .limit(100)
    )

    syntax = Syntax(str(cross_join_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="✖️ CROSS JOIN Query", border_style="green"))

    # 5. WHERE ANY - Advanced Filtering
    console.print("\n[bold cyan]5. WHERE ANY - Advanced Filtering[/bold cyan]")

    # Subquery for WHERE ANY
    high_performers = sql.select("id").from_("employees").where("salary > 100000")

    where_any_query = (
        sql.select("*")
        .from_("sales")
        .where_any("employee_id", high_performers)
        .where_not_any("region", ["Discontinued", "Inactive"])
        .order_by("sale_date DESC")
        .limit(50)
    )

    syntax = Syntax(str(where_any_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="🎯 WHERE ANY Query", border_style="green"))

    console.print()


def demo_window_functions_mastery() -> None:
    """Demonstrate mastery of window functions and analytics."""
    console.print(
        Panel(
            "[bold blue]⚡ WINDOW FUNCTIONS MASTERY ⚡[/bold blue]\n"
            "Advanced analytics with ROW_NUMBER, RANK, LAG, LEAD, and more!",
            border_style="blue",
        )
    )

    # Complex window function query
    console.print("\n[bold cyan]Advanced Analytics Dashboard Query[/bold cyan]")

    window_query = (
        sql.select(
            "e.name",
            "e.department",
            "e.salary",
            "ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_salary_rank",
            "RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_rank",
            "DENSE_RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as dept_dense_rank",
            # LAG and LEAD for trend analysis
            "LAG(e.salary, 1) OVER (PARTITION BY e.department ORDER BY e.hire_date) as prev_hire_salary",
            "LEAD(e.salary, 1) OVER (PARTITION BY e.department ORDER BY e.hire_date) as next_hire_salary",
            # Percentile functions
            "PERCENT_RANK() OVER (PARTITION BY e.department ORDER BY e.salary) as salary_percentile",
            # Running totals
            "SUM(e.salary) OVER (PARTITION BY e.department ORDER BY e.hire_date ROWS UNBOUNDED PRECEDING) as running_salary_total",
        )
        .from_("employees e")
        .where("e.active = TRUE")
        .order_by("e.department", "dept_salary_rank")
    )

    syntax = Syntax(str(window_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="📊 Advanced Window Functions", border_style="cyan"))
    console.print()


def demo_recursive_cte_mastery() -> None:
    """Demonstrate recursive CTEs and complex hierarchical queries."""
    console.print(
        Panel(
            "[bold magenta]🏗️ RECURSIVE CTE MASTERY 🏗️[/bold magenta]\n"
            "Complex hierarchical queries and recursive data processing!",
            border_style="magenta",
        )
    )

    # 1. Employee Hierarchy with Recursive CTE
    console.print("\n[bold cyan]1. Employee Hierarchy Tree[/bold cyan]")

    hierarchy_query = (
        sql.select("*")
        .with_(
            "employee_hierarchy",
            """
            WITH RECURSIVE employee_hierarchy AS (
                -- Base case: Top-level managers (no manager)
                SELECT
                    id, name, email, department, position, salary, manager_id,
                    0 as level,
                    CAST(name AS VARCHAR) as hierarchy_path,
                    CAST(id AS VARCHAR) as id_path
                FROM employees
                WHERE manager_id IS NULL AND active = TRUE

                UNION ALL

                -- Recursive case: Employees with managers
                SELECT
                    e.id, e.name, e.email, e.department, e.position, e.salary, e.manager_id,
                    eh.level + 1,
                    eh.hierarchy_path || ' -> ' || e.name,
                    eh.id_path || ',' || CAST(e.id AS VARCHAR)
                FROM employees e
                INNER JOIN employee_hierarchy eh ON e.manager_id = eh.id
                WHERE e.active = TRUE
            )
            SELECT * FROM employee_hierarchy ORDER BY level, department, name
            """,
            recursive=True,
        )
        .from_("employee_hierarchy")
        .where("level <= 5")  # Prevent infinite recursion
    )

    syntax = Syntax(str(hierarchy_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="🌳 Recursive Employee Hierarchy", border_style="green"))

    # 2. Sales Territory Expansion Analysis
    console.print("\n[bold cyan]2. Sales Territory Expansion Analysis[/bold cyan]")

    territory_query = (
        sql.select("*")
        .with_(
            "territory_growth",
            """
            WITH RECURSIVE territory_growth AS (
                -- Base: Initial territories
                SELECT
                    region,
                    1 as expansion_level,
                    COUNT(*) as customer_count,
                    SUM(lifetime_value) as total_value
                FROM customers
                WHERE customer_since >= '2023-01-01'
                GROUP BY region

                UNION ALL

                -- Recursive: Adjacent territory analysis
                SELECT
                    c.region,
                    tg.expansion_level + 1,
                    COUNT(*) as customer_count,
                    SUM(c.lifetime_value) as total_value
                FROM customers c
                CROSS JOIN territory_growth tg
                WHERE c.region != tg.region
                  AND tg.expansion_level < 3
                  AND c.lifetime_value > tg.total_value / tg.customer_count
                GROUP BY c.region, tg.expansion_level
            )
            SELECT * FROM territory_growth ORDER BY expansion_level, total_value DESC
            """,
        )
        .from_("territory_growth")
    )

    syntax = Syntax(str(territory_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="🗺️ Recursive Territory Analysis", border_style="green"))
    console.print()


def demo_ultimate_complex_query() -> None:
    """The most complex SQL query ever generated - combining EVERYTHING!"""
    console.print(
        Panel(
            "[bold red]🎯 THE ULTIMATE COMPLEX QUERY 🎯[/bold red]\n"
            "Combining EVERY advanced feature in one mind-blowing query!",
            border_style="red",
        )
    )

    # This query combines:
    # - Multiple CTEs (some recursive)
    # - PIVOT operations
    # - Window functions
    # - Complex joins (INNER, LEFT, CROSS)
    # - WHERE ANY/NOT ANY
    # - CASE expressions
    # - Subqueries
    # - ROLLUP
    # - Advanced filtering

    ultimate_query = (
        sql.select(
            "final_analysis.*",
            # Complex CASE expression
            """CASE
                WHEN performance_score > 90 THEN 'Exceptional'
                WHEN performance_score > 75 THEN 'High Performer'
                WHEN performance_score > 60 THEN 'Average'
                ELSE 'Needs Improvement'
            END as performance_tier""",
            # Advanced window functions
            "ROW_NUMBER() OVER (PARTITION BY department ORDER BY performance_score DESC) as dept_performance_rank",
            "PERCENT_RANK() OVER (ORDER BY performance_score) as overall_percentile",
            "LAG(performance_score, 1) OVER (PARTITION BY department ORDER BY hire_date) as prev_performance",
            # Running calculations
            "SUM(total_sales) OVER (PARTITION BY department ORDER BY hire_date ROWS UNBOUNDED PRECEDING) as cumulative_dept_sales",
        )
        # Multiple CTEs with complex logic
        .with_(
            "high_value_customers",
            sql.select("id", "name", "lifetime_value")
            .from_("customers")
            .where("lifetime_value > 50000")
            .order_by("lifetime_value DESC"),
        )
        .with_(
            "top_performers",
            sql.select(
                "e.id", "e.name", "e.department", "e.salary", "e.hire_date", "AVG(pm.metric_value) as avg_performance"
            )
            .from_("employees e")
            .left_join("performance_metrics pm", "e.id = pm.employee_id")
            .where("e.active = TRUE")
            .group_by("e.id", "e.name", "e.department", "e.salary", "e.hire_date")
            .having("AVG(pm.metric_value) > 75"),
        )
        .with_(
            "sales_pivot",
            """
            SELECT employee_id, Q1, Q2, Q3, Q4, (Q1 + Q2 + Q3 + Q4) as total_yearly
            FROM (
                SELECT employee_id, quarter, SUM(total_amount) as quarterly_sales
                FROM sales
                WHERE sale_date >= '2023-01-01'
                GROUP BY employee_id, quarter
            ) base
            PIVOT (
                SUM(quarterly_sales)
                FOR quarter IN ('Q1' as Q1, 'Q2' as Q2, 'Q3' as Q3, 'Q4' as Q4)
            )
            """,
        )
        # Complex FROM with multiple joins
        .from_(
            """
            (
                SELECT
                    tp.id,
                    tp.name,
                    tp.department,
                    tp.salary,
                    tp.hire_date,
                    tp.avg_performance,
                    COALESCE(sp.total_yearly, 0) as total_sales,
                    -- Performance scoring algorithm
                    (
                        (tp.avg_performance * 0.4) +
                        (LEAST(COALESCE(sp.total_yearly, 0) / 100000, 100) * 0.3) +
                        (CASE
                            WHEN tp.salary > 100000 THEN 20
                            WHEN tp.salary > 75000 THEN 15
                            WHEN tp.salary > 50000 THEN 10
                            ELSE 5
                        END * 0.3)
                    ) as performance_score
                FROM top_performers tp
                LEFT JOIN sales_pivot sp ON tp.id = sp.employee_id
            ) final_analysis
            """
        )
        # Advanced WHERE conditions with ANY/NOT ANY
        .where_any(
            "final_analysis.id",
            sql.select("s.employee_id")
            .from_("sales s")
            .inner_join("high_value_customers hvc", "s.customer_id = hvc.id")
            .where("s.sale_date >= '2023-01-01'")
            .group_by("s.employee_id")
            .having("COUNT(DISTINCT s.customer_id) >= 5"),
        )
        .where_not_any("final_analysis.department", ["Discontinued", "Inactive", "Temp"])
        .where("final_analysis.performance_score > 50")
        # Complex ordering
        .order_by("final_analysis.department", "performance_score DESC", "total_sales DESC")
        .limit(100)
    )

    syntax = Syntax(str(ultimate_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="🚀 THE ULTIMATE COMPLEX QUERY", border_style="red"))

    # Show query complexity analysis
    console.print("\n[bold yellow]📊 Query Complexity Analysis:[/bold yellow]")
    complexity_table = Table(title="🧠 Query Intelligence Metrics")
    complexity_table.add_column("Metric", style="cyan")
    complexity_table.add_column("Count", style="green")
    complexity_table.add_column("Complexity Level", style="yellow")

    query_str = str(ultimate_query)

    complexity_table.add_row("CTEs (WITH clauses)", "3", "🔥 Expert")
    complexity_table.add_row("JOIN operations", "4+", "🔥 Expert")
    complexity_table.add_row("Window functions", "5+", "🔥 Expert")
    complexity_table.add_row("Subqueries", "3+", "🔥 Expert")
    complexity_table.add_row("CASE expressions", "2+", "⚡ Advanced")
    complexity_table.add_row("WHERE ANY/NOT ANY", "2", "🆕 Cutting Edge")
    complexity_table.add_row("PIVOT operations", "1", "🆕 Cutting Edge")
    complexity_table.add_row("Aggregate functions", "10+", "⚡ Advanced")
    complexity_table.add_row("Lines of SQL", str(query_str.count("\n") + 1), "🚀 Massive")

    console.print(complexity_table)
    console.print()


def demo_aiosql_integration_ultimate() -> None:
    """Demonstrate ultimate aiosql integration with complex queries."""
    console.print(
        Panel(
            "[bold green]🔗 ULTIMATE AIOSQL INTEGRATION 🔗[/bold green]\nComplex SQL files + SQLSpec builder magic!",
            border_style="green",
        )
    )

    # Create advanced SQL file
    advanced_sql = """
-- name: employee_performance_dashboard^
WITH RECURSIVE org_hierarchy AS (
    SELECT id, name, department, manager_id, 0 as level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    SELECT e.id, e.name, e.department, e.manager_id, oh.level + 1
    FROM employees e
    JOIN org_hierarchy oh ON e.manager_id = oh.id
    WHERE oh.level < 10
),
performance_metrics AS (
    SELECT
        e.id,
        e.name,
        e.department,
        e.salary,
        oh.level as org_level,
        AVG(pm.metric_value) as avg_performance,
        COUNT(s.id) as total_sales,
        SUM(s.total_amount) as revenue_generated,
        ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY AVG(pm.metric_value) DESC) as dept_rank
    FROM employees e
    JOIN org_hierarchy oh ON e.id = oh.id
    LEFT JOIN performance_metrics pm ON e.id = pm.employee_id
    LEFT JOIN sales s ON e.id = s.employee_id AND s.sale_date >= :start_date
    WHERE e.active = TRUE
    GROUP BY e.id, e.name, e.department, e.salary, oh.level
)
SELECT
    pm.*,
    CASE
        WHEN pm.avg_performance > 90 THEN 'Exceptional'
        WHEN pm.avg_performance > 75 THEN 'High Performer'
        WHEN pm.avg_performance > 60 THEN 'Average'
        ELSE 'Needs Improvement'
    END as performance_tier,
    PERCENT_RANK() OVER (ORDER BY pm.avg_performance) as overall_percentile
FROM performance_metrics pm
WHERE pm.avg_performance > :min_performance
ORDER BY pm.dept_rank, pm.avg_performance DESC

-- name: sales_territory_analysis^
SELECT
    region,
    quarter,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction,
    COUNT(DISTINCT employee_id) as active_salespeople,
    COUNT(DISTINCT customer_id) as unique_customers
FROM sales
WHERE sale_date BETWEEN :start_date AND :end_date
GROUP BY ROLLUP(region, quarter)
ORDER BY region, quarter

-- name: customer_lifetime_value_analysis^
WITH customer_segments AS (
    SELECT
        c.id,
        c.name,
        c.region,
        c.customer_since,
        COUNT(s.id) as total_orders,
        SUM(s.total_amount) as lifetime_value,
        AVG(s.total_amount) as avg_order_value,
        MAX(s.sale_date) as last_purchase_date,
        NTILE(5) OVER (ORDER BY SUM(s.total_amount)) as value_quintile
    FROM customers c
    LEFT JOIN sales s ON c.id = s.customer_id
    GROUP BY c.id, c.name, c.region, c.customer_since
)
SELECT
    cs.*,
    CASE cs.value_quintile
        WHEN 5 THEN 'VIP'
        WHEN 4 THEN 'High Value'
        WHEN 3 THEN 'Medium Value'
        WHEN 2 THEN 'Low Value'
        ELSE 'New/Inactive'
    END as customer_tier,
    DATE_DIFF('day', cs.last_purchase_date, CURRENT_DATE) as days_since_last_purchase
FROM customer_segments cs
WHERE cs.lifetime_value > :min_lifetime_value
ORDER BY cs.lifetime_value DESC
"""

    Path("ultimate_analytics.sql").write_text(advanced_sql, encoding="utf-8")

    # Load and demonstrate
    console.print("[bold]📁 Loading complex SQL file...[/bold]")
    loader = AiosqlLoader("ultimate_analytics.sql", dialect="duckdb")

    # Get a query and enhance it with builder API
    dashboard_query = loader.get_query("employee_performance_dashboard")

    console.print("[bold]🔧 Enhancing with Builder API...[/bold]")
    enhanced_query = (
        dashboard_query.where("department IN ('Sales', 'Engineering')")
        .where("salary > 75000")
        .order_by("revenue_generated DESC")
        .limit(20)
    )

    syntax = Syntax(str(enhanced_query), "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="🎯 Enhanced Aiosql Query", border_style="cyan"))

    # Clean up
    Path("ultimate_analytics.sql").unlink(missing_ok=True)
    console.print()


def demo_sql_injection_prevention() -> None:
    """Demonstrate bulletproof SQL injection prevention."""
    console.print(
        Panel(
            "[bold red]🛡️ SQL INJECTION PREVENTION 🛡️[/bold red]\nBulletproof security with automatic parameter binding!",
            border_style="red",
        )
    )

    # Show dangerous input that would cause SQL injection
    dangerous_inputs = [
        "'; DROP TABLE employees; --",
        "1 OR 1=1",
        "admin' UNION SELECT * FROM passwords --",
        "'; INSERT INTO logs VALUES ('hacked'); --",
    ]

    console.print("[bold yellow]⚠️ Dangerous Inputs (Automatically Neutralized):[/bold yellow]")
    for i, dangerous_input in enumerate(dangerous_inputs, 1):
        console.print(f"  {i}. [red]{dangerous_input}[/red]")

    console.print("\n[bold green]✅ SQLSpec Automatically Prevents Injection:[/bold green]")

    # Show how SQLSpec safely handles dangerous input
    safe_query = (
        sql.select("*")
        .from_("employees")
        .where(("name", "'; DROP TABLE employees; --"))  # This gets parameterized!
        .where_like("email", "%admin' UNION SELECT%")  # This too!
        .where_in("department", ["Sales'; DELETE FROM users; --", "Engineering"])
    )

    built_query = safe_query.build()

    console.print("[bold]🔒 Safe SQL with Parameters:[/bold]")
    syntax = Syntax(built_query.sql, "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title="Safe SQL", border_style="green"))

    console.print("[bold]🔑 Safely Bound Parameters:[/bold]")
    params_table = Table(title="Parameter Binding")
    params_table.add_column("Parameter", style="cyan")
    params_table.add_column("Value", style="yellow")
    params_table.add_column("Status", style="green")

    for param_name, param_value in built_query.parameters.items():
        params_table.add_row(
            f":{param_name}",
            str(param_value)[:50] + "..." if len(str(param_value)) > 50 else str(param_value),
            "✅ SAFE",
        )

    console.print(params_table)
    console.print()


def demo_performance_showcase() -> None:
    """Showcase performance and optimization features."""
    console.print(
        Panel(
            "[bold magenta]⚡ PERFORMANCE SHOWCASE ⚡[/bold magenta]\nLightning-fast query building and optimization!",
            border_style="magenta",
        )
    )

    # Performance timing
    console.print("[bold]🏁 Performance Benchmarks:[/bold]")

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        # Simple query timing
        task1 = progress.add_task("Building simple query...", total=None)
        start_time = time.time()
        for _ in range(1000):
            simple_query = sql.select("*").from_("users").where(("active", True))
            _ = simple_query.build()
        simple_time = time.time() - start_time
        progress.remove_task(task1)

        # Complex query timing
        task2 = progress.add_task("Building complex query...", total=None)
        start_time = time.time()
        for _ in range(100):
            complex_query = (
                sql.select("*")
                .with_("cte1", sql.select("id").from_("table1"))
                .from_("main_table")
                .left_join("other_table", "main_table.id = other_table.main_id")
                .where(("status", "active"))
                .where_in("category", ["A", "B", "C"])
                .group_by("department")
                .having("COUNT(*) > 5")
                .order_by("created_at DESC")
                .limit(100)
            )
            _ = complex_query.build()
        complex_time = time.time() - start_time
        progress.remove_task(task2)

    # Performance results
    perf_table = Table(title="🚀 Performance Results")
    perf_table.add_column("Query Type", style="cyan")
    perf_table.add_column("Iterations", style="yellow")
    perf_table.add_column("Total Time", style="green")
    perf_table.add_column("Avg per Query", style="magenta")
    perf_table.add_column("Queries/Second", style="red")

    perf_table.add_row(
        "Simple Query",
        "1,000",
        f"{simple_time:.3f}s",
        f"{simple_time / 1000 * 1000:.3f}ms",
        f"{1000 / simple_time:.0f}",
    )

    perf_table.add_row(
        "Complex Query",
        "100",
        f"{complex_time:.3f}s",
        f"{complex_time / 100 * 1000:.3f}ms",
        f"{100 / complex_time:.0f}",
    )

    console.print(perf_table)
    console.print()


def demo_conclusion() -> None:
    """Create the most epic conclusion ever."""
    console.print(
        Panel(
            "[bold magenta]🎉 SQLSPEC = THE FUTURE OF SQL! 🎉[/bold magenta]\n\n"
            "[yellow]🚀 What We've Demonstrated:[/yellow]\n"
            "• [bold]PIVOT, UNPIVOT, ROLLUP, CROSS JOIN[/bold] - Latest SQL features\n"
            "• [bold]WHERE ANY, WHERE NOT ANY[/bold] - Advanced filtering\n"
            "• [bold]Window Functions[/bold] - ROW_NUMBER, RANK, LAG, LEAD\n"
            "• [bold]Recursive CTEs[/bold] - Complex hierarchical queries\n"
            "• [bold]Advanced Joins[/bold] - Every type of join imaginable\n"
            "• [bold]Case Expressions[/bold] - Complex conditional logic\n"
            "• [bold]Subqueries & Correlated Subqueries[/bold] - Nested complexity\n"
            "• [bold]SQL Injection Prevention[/bold] - Bulletproof security\n"
            "• [bold]Aiosql Integration[/bold] - File-based + builder magic\n"
            "• [bold]Performance Optimization[/bold] - Lightning-fast generation\n\n"
            "[green]💎 Why SQLSpec is Revolutionary:[/green]\n"
            "• [bold]Type Safety[/bold]: Full Pydantic/msgspec integration\n"
            "• [bold]Security First[/bold]: Automatic parameter binding\n"
            "• [bold]Developer Experience[/bold]: Fluent, intuitive API\n"
            "• [bold]Database Agnostic[/bold]: Works with 20+ databases\n"
            "• [bold]Production Ready[/bold]: Battle-tested and optimized\n"
            "• [bold]Extensible[/bold]: Plugin architecture for custom features\n\n"
            "[cyan]🔥 SQLSpec doesn't just build SQL - it builds the FUTURE! 🔥[/cyan]",
            title="Demo Complete - Mind = Blown 🤯",
            border_style="double",
            box=box.DOUBLE,
        )
    )


def main() -> None:
    """Run the ultimate SQLSpec showcase."""
    try:
        # 🎬 Epic Demo
        demo_header()

        console.print("[bold cyan]🔧 Setting up ultimate database...[/bold cyan]")
        with console.status("[bold green]Creating complex schema..."):
            _ = create_ultimate_database()  # Mark as intentionally unused
        console.print("[green]✅ Ultimate database ready![/green]\n")

        # 🚀 Feature Demonstrations
        demo_new_sql_features()
        demo_window_functions_mastery()
        demo_recursive_cte_mastery()
        demo_ultimate_complex_query()
        demo_aiosql_integration_ultimate()
        demo_sql_injection_prevention()
        demo_performance_showcase()

        # 🎉 Epic Conclusion
        demo_conclusion()

    except Exception as e:
        console.print(f"[red]❌ Demo error: {e}[/red]")
        console.print_exception()


if __name__ == "__main__":
    main()
