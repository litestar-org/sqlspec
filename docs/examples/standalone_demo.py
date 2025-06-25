#!/usr/bin/env python3
# /// script
# dependencies = [
#   "sqlspec[duckdb,performance] @ git+https://github.com/litestar-org/sqlspec.git@main",
#   "rich>=13.0.0",
#   "rich-click>=1.7.0",
#   "faker>=24.0.0",
#   "pydantic>=2.0.0",
#   "click>=8.0.0",
# ]
# ///

"""SQLSpec Interactive Demo - Showcase of Advanced SQL Generation & Processing

A comprehensive demonstration of SQLSpec's capabilities including:
- Advanced SQL query construction patterns
- Filter composition and pipeline processing
- Statement analysis and validation
- Performance instrumentation and monitoring

This demo uses rich-click for an interactive CLI experience.
"""

import time
from datetime import datetime
from decimal import Decimal
from typing import Any

import rich_click as rclick
from faker import Faker
from pydantic import BaseModel, Field
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from sqlspec import sql
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.statement.filters import LimitOffsetFilter, OrderByFilter, SearchFilter
from sqlspec.statement.sql import SQL, SQLConfig

# Display constants
MAX_ROWS_TO_DISPLAY = 5

__all__ = (
    "Order",
    "Product",
    "User",
    "analysis",
    "builders",
    "cli",
    "create_sample_database",
    "demo_basic_select",
    "demo_complex_joins",
    "demo_cte_queries",
    "demo_insert_returning",
    "demo_merge_operations",
    "demo_subqueries",
    "demo_update_joins",
    "demo_window_functions",
    "display_header",
    "display_sql_with_syntax",
    "filters",
    "interactive",
    "performance",
    "show_interactive_examples",
    "show_interactive_help",
)


# Configure rich-click
rclick.rich_click.USE_RICH_MARKUP = True
rclick.rich_click.USE_MARKDOWN = True
rclick.rich_click.SHOW_ARGUMENTS = True
rclick.rich_click.GROUP_ARGUMENTS_OPTIONS = True

console = Console()
fake = Faker()


# Data Models for Demo
class User(BaseModel):
    id: int
    name: str = Field(min_length=2, max_length=100)
    email: str = Field(pattern=r"^[^@]+@[^@]+\.[^@]+$")
    department: str
    age: int = Field(ge=18, le=120)
    salary: Decimal = Field(ge=0, decimal_places=2)
    hire_date: datetime
    active: bool = True


class Product(BaseModel):
    id: int
    name: str
    category: str
    price: Decimal = Field(ge=0, decimal_places=2)
    stock_quantity: int = Field(ge=0)
    created_at: datetime


class Order(BaseModel):
    id: int
    user_id: int
    product_id: int
    quantity: int = Field(ge=1)
    total_amount: Decimal = Field(ge=0, decimal_places=2)
    order_date: datetime
    status: str


def create_sample_database() -> Any:
    """Create a sample DuckDB database with realistic data."""
    config = DuckDBConfig()

    with config.provide_session() as driver:
        # Create comprehensive schema
        driver.execute(
            SQL("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                email VARCHAR UNIQUE,
                department VARCHAR,
                age INTEGER,
                salary DECIMAL(10,2),
                hire_date TIMESTAMP,
                active BOOLEAN DEFAULT TRUE
            )
        """)
        )

        driver.execute(
            SQL("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                category VARCHAR,
                price DECIMAL(10,2),
                stock_quantity INTEGER,
                created_at TIMESTAMP
            )
        """)
        )

        driver.execute(
            SQL("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                total_amount DECIMAL(10,2),
                order_date TIMESTAMP,
                status VARCHAR
            )
        """)
        )

        # Generate sample data
        departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"]
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
        statuses = ["pending", "shipped", "delivered", "cancelled"]

        # Insert users
        for i in range(1, 51):
            driver.execute(
                SQL(
                    """
                    INSERT INTO users (id, name, email, department, age, salary, hire_date, active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    parameters=(
                        i,
                        fake.name(),
                        fake.unique.email(),
                        fake.random_element(departments),
                        fake.random_int(min=22, max=65),
                        fake.random_int(min=40000, max=150000),
                        fake.date_between(start_date="-3y", end_date="today"),
                        fake.boolean(chance_of_getting_true=85),
                    ),
                )
            )

        # Insert products
        for i in range(1, 31):
            driver.execute(
                SQL(
                    """
                    INSERT INTO products (id, name, category, price, stock_quantity, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    parameters=(
                        i,
                        fake.catch_phrase(),
                        fake.random_element(categories),
                        fake.random_int(min=10, max=1000),
                        fake.random_int(min=0, max=100),
                        fake.date_time_between(start_date="-2y", end_date="now"),
                    ),
                )
            )

        # Insert orders
        for i in range(1, 101):
            quantity = fake.random_int(min=1, max=5)
            price = fake.random_int(min=10, max=500)
            driver.execute(
                SQL(
                    """
                    INSERT INTO orders (id, user_id, product_id, quantity, total_amount, order_date, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    parameters=(
                        i,
                        fake.random_int(min=1, max=50),
                        fake.random_int(min=1, max=30),
                        quantity,
                        quantity * price,
                        fake.date_time_between(start_date="-1y", end_date="now"),
                        fake.random_element(statuses),
                    ),
                )
            )

    return config


def display_header() -> None:
    """Display the demo header."""
    header = Panel.fit(
        "[bold blue]SQLSpec Interactive Demo[/bold blue]\n"
        "[cyan]Advanced SQL Generation & Processing Framework[/cyan]\n\n"
        "Explore SQL builders, filters, validation, and analysis",
        title="Welcome",
        border_style="blue",
        box=box.DOUBLE,
    )
    console.print(header)


def display_sql_with_syntax(sql_obj: SQL, title: str = "Generated SQL") -> None:
    """Display SQL with syntax highlighting."""
    sql_text = str(sql_obj)
    syntax = Syntax(sql_text, "sql", theme="monokai", line_numbers=True)
    console.print(Panel(syntax, title=title, border_style="green"))

    # Show parameters if any
    if sql_obj.parameters:
        params_table = Table(title="Parameters")
        params_table.add_column("Name", style="cyan")
        params_table.add_column("Value", style="yellow")

        if isinstance(sql_obj.parameters, dict):
            for name, value in sql_obj.parameters.items():
                params_table.add_row(str(name), str(value))
        elif isinstance(sql_obj.parameters, (list, tuple)):
            for i, value in enumerate(sql_obj.parameters):
                params_table.add_row(f"${i + 1}", str(value))
        else:
            params_table.add_row("value", str(sql_obj.parameters))

        console.print(params_table)


@rclick.group()
@rclick.version_option()
def cli() -> None:
    """SQLSpec Interactive Demo - Showcase Advanced SQL Capabilities"""
    display_header()


@cli.command()
def builders() -> None:
    """Demonstrate SQL builder patterns and advanced query construction."""
    console.print(
        Panel(
            "[bold green]SQL Builder Demonstrations[/bold green]\n"
            "Showcasing fluent query builders with advanced features",
            border_style="green",
        )
    )

    demos = [
        ("Basic SELECT with WHERE and ORDER BY", demo_basic_select),
        ("Complex JOIN with aggregations", demo_complex_joins),
        ("Window functions and analytics", demo_window_functions),
        ("CTEs and recursive queries", demo_cte_queries),
        ("INSERT with RETURNING", demo_insert_returning),
        ("UPDATE with JOINs", demo_update_joins),
        ("MERGE/UPSERT operations", demo_merge_operations),
        ("Subqueries and EXISTS", demo_subqueries),
    ]

    for title, demo_func in demos:
        console.print(f"\n[bold cyan]{title}[/bold cyan]")
        console.print("─" * 50)
        demo_func()


@cli.command()
def filters() -> None:
    """Demonstrate filter composition and SQL transformation."""
    console.print(
        Panel(
            "[bold magenta]Filter System Demo[/bold magenta]\nComposable filters for dynamic SQL modification",
            border_style="magenta",
        )
    )

    # Base query
    base_query = sql.select("id", "name", "email", "department", "salary").from_("users")

    console.print("[bold cyan]1. Base query[/bold cyan]")
    display_sql_with_syntax(base_query, "Base Query")

    # Apply various filters
    filters_demo = [
        ("Search Filter", SearchFilter("name", "John")),
        ("Pagination", LimitOffsetFilter(10, 20)),
        ("Ordering", OrderByFilter("salary", "desc")),
    ]

    for title, filter_obj in filters_demo:
        console.print(f"\n[bold cyan]2. With {title}[/bold cyan]")
        filtered_query = base_query.append_filter(filter_obj)
        display_sql_with_syntax(filtered_query, f"Query with {title}")

    # Combined filters
    console.print("\n[bold cyan]3. Combined filters[/bold cyan]")
    combined_query = base_query.copy(
        SearchFilter("department", "Engineering"), LimitOffsetFilter(5, 0), OrderByFilter("hire_date", "desc")
    )
    display_sql_with_syntax(combined_query, "Query with Combined Filters")


@cli.command()
def analysis() -> None:
    """Demonstrate SQL analysis and validation pipeline."""
    console.print(
        Panel(
            "[bold red]Analysis & Validation Demo[/bold red]\n"
            "SQL statement analysis, validation, and optimization insights",
            border_style="red",
        )
    )

    # Create analyzer with custom config
    config = SQLConfig(enable_analysis=True, enable_validation=True, enable_transformations=True)

    # Demo queries with different complexity levels
    queries = [
        ("Simple Query", "SELECT * FROM users WHERE active = TRUE"),
        (
            "Complex Join",
            """
            SELECT u.name, COUNT(o.id) as order_count, SUM(o.total_amount) as total_spent
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.active = TRUE
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 5
            ORDER BY total_spent DESC
        """,
        ),
        ("Risky Query", "UPDATE users SET salary = salary * 1.1"),  # No WHERE clause
        (
            "Complex Analytics",
            """
            WITH RECURSIVE employee_hierarchy AS (
                SELECT id, name, manager_id, 0 as level
                FROM employees
                WHERE manager_id IS NULL
                UNION ALL
                SELECT e.id, e.name, e.manager_id, eh.level + 1
                FROM employees e
                JOIN employee_hierarchy eh ON e.manager_id = eh.id
                WHERE eh.level < 10
            ),
            sales_summary AS (
                SELECT
                    e.id,
                    e.name,
                    e.level,
                    COUNT(s.id) as sale_count,
                    SUM(s.amount) as total_sales,
                    AVG(s.amount) as avg_sale,
                    ROW_NUMBER() OVER (PARTITION BY e.level ORDER BY SUM(s.amount) DESC) as rank_in_level
                FROM employee_hierarchy e
                LEFT JOIN sales s ON e.id = s.employee_id
                GROUP BY e.id, e.name, e.level
            )
            SELECT * FROM sales_summary WHERE rank_in_level <= 3
        """,
        ),
    ]

    for title, sql_text in queries:
        console.print(f"\n[bold cyan]{title}[/bold cyan]")
        console.print("─" * 50)

        # Create SQL object with analysis
        stmt = SQL(sql_text, config=config)

        # Display the SQL
        display_sql_with_syntax(stmt, f"{title} - SQL")

        # Show validation results
        validation = stmt.validate()
        if validation:
            validation_table = Table(title="Validation Results")
            validation_table.add_column("Aspect", style="cyan")
            validation_table.add_column("Status", style="green")
            validation_table.add_column("Details", style="yellow")

            validation_table.add_row(
                "Safety", "✓ Safe" if validation.is_safe else "⚠ Issues Found", f"Risk Level: {validation.risk_level}"
            )

            if validation.issues:
                for issue in validation.issues:
                    validation_table.add_row("Issue", "⚠ Warning", issue)

            if validation.warnings:
                for warning in validation.warnings:
                    validation_table.add_row("Warning", "i Info", warning)

            console.print(validation_table)

        # Show analysis results if available
        if stmt.analysis_result:
            analysis = stmt.analysis_result

            analysis_table = Table(title="Analysis Results")
            analysis_table.add_column("Metric", style="cyan")
            analysis_table.add_column("Value", style="green")

            analysis_table.add_row("Statement Type", analysis.statement_type)
            analysis_table.add_row("Tables", ", ".join(analysis.tables))
            analysis_table.add_row("Join Count", str(analysis.join_count))
            analysis_table.add_row("Uses Subqueries", str(analysis.uses_subqueries))
            analysis_table.add_row("Complexity Score", str(analysis.complexity_score))

            if analysis.aggregate_functions:
                analysis_table.add_row("Aggregate Functions", ", ".join(analysis.aggregate_functions))

            console.print(analysis_table)


@cli.command()
@rclick.option("--count", default=1000, help="Number of queries to generate for performance test")
def performance() -> None:
    """Demonstrate performance characteristics and optimizations."""
    console.print(
        Panel(
            "[bold green]Performance Demo[/bold green]\nShowcasing SQLSpec's performance optimizations",
            border_style="green",
        )
    )

    # Performance test scenarios
    scenarios = [
        ("Simple Query Building", lambda: sql.select("*").from_("users").where(("active", True))),
        (
            "Complex Query Building",
            lambda: (
                sql.select("u.name", "COUNT(o.id) as orders", "SUM(o.amount) as total")
                .from_("users u")
                .left_join("orders o", "u.id = o.user_id")
                .where("u.active = TRUE")
                .group_by("u.id", "u.name")
                .having("COUNT(o.id) > 5")
                .order_by("total DESC")
                .limit(10)
            ),
        ),
        (
            "Parameter Binding",
            lambda: sql.select("*").from_("users").where(("salary", ">", fake.random_int(50000, 100000))),
        ),
    ]

    count = 1000

    console.print(f"[bold yellow]Running performance tests ({count:,} iterations each)...[/bold yellow]\n")

    results_table = Table(title="Performance Results")
    results_table.add_column("Scenario", style="cyan")
    results_table.add_column("Total Time", style="green")
    results_table.add_column("Avg per Query", style="yellow")
    results_table.add_column("Queries/Second", style="magenta")

    for scenario_name, query_func in scenarios:
        with console.status(f"[bold green]Testing {scenario_name}..."):
            start_time = time.time()

            for _ in range(count):
                query = query_func()
                _ = str(query)  # Force SQL generation

            end_time = time.time()
            total_time = end_time - start_time
            avg_time = total_time / count
            qps = count / total_time

        results_table.add_row(scenario_name, f"{total_time:.3f}s", f"{avg_time * 1000:.3f}ms", f"{qps:.0f}")

    console.print(results_table)


# Demo functions for builders
def demo_basic_select() -> None:
    """Demonstrate basic SELECT query building."""
    query = (
        sql.select("id", "name", "email", "department", "salary")
        .from_("users")
        .where("active = TRUE")
        .where("salary > ?", 50000)
        .order_by("salary DESC", "hire_date")
        .limit(10)
    )
    display_sql_with_syntax(query)


def demo_complex_joins() -> None:
    """Demonstrate complex JOIN operations."""
    query = (
        sql.select(
            "u.name",
            "u.department",
            "COUNT(o.id) as order_count",
            "SUM(o.total_amount) as total_spent",
            "AVG(o.total_amount) as avg_order_value",
        )
        .from_("users u")
        .inner_join("orders o", "u.id = o.user_id")
        .inner_join("products p", "o.product_id = p.id")
        .where("u.active = TRUE")
        .where("o.status IN ('shipped', 'delivered')")
        .group_by("u.id", "u.name", "u.department")
        .having("COUNT(o.id) > 3")
        .order_by("total_spent DESC")
        .limit(20)
    )
    display_sql_with_syntax(query)


def demo_window_functions() -> None:
    """Demonstrate window functions and analytics."""
    query = (
        sql.select(
            "name",
            "department",
            "salary",
            "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank",
            "RANK() OVER (ORDER BY salary DESC) as overall_rank",
            "LAG(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) as prev_salary",
            "SUM(salary) OVER (PARTITION BY department) as dept_total_salary",
        )
        .from_("users")
        .where("active = TRUE")
    )

    display_sql_with_syntax(query)


def demo_cte_queries() -> None:
    """Demonstrate CTEs and recursive queries."""
    cte_query = sql.select("department", "AVG(salary) as avg_salary").from_("users").group_by("department")

    query = (
        sql.select("u.name", "u.salary", "ds.avg_salary", "(u.salary - ds.avg_salary) as salary_diff")
        .with_("dept_stats", cte_query)
        .from_("users u")
        .inner_join("dept_stats ds", "u.department = ds.department")
        .where("u.active = TRUE")
        .order_by("salary_diff DESC")
    )
    display_sql_with_syntax(query)


def demo_insert_returning() -> None:
    """Demonstrate INSERT with RETURNING."""
    query = SQL(
        """
        INSERT INTO users (name, email, department, age, salary, hire_date)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id, name, email
    """,
        parameters=("John Doe", "john@example.com", "Engineering", 30, 75000, datetime.now()),
    )

    display_sql_with_syntax(query)


def demo_update_joins() -> None:
    """Demonstrate UPDATE with JOINs."""
    query = SQL("""
        UPDATE users u
        SET salary = u.salary * 1.1
        FROM orders o
        WHERE u.id = o.user_id
          AND o.status = 'delivered'
          AND u.department = 'Sales'
        RETURNING u.id, u.name, u.salary
    """)

    display_sql_with_syntax(query)


def demo_merge_operations() -> None:
    """Demonstrate MERGE/UPSERT operations."""
    # DuckDB uses INSERT with ON CONFLICT for upsert
    query = SQL("""
        INSERT INTO users (id, name, email, salary)
        SELECT id, name, email, salary FROM temp_users
        ON CONFLICT (email) DO UPDATE SET
            name = EXCLUDED.name,
            salary = EXCLUDED.salary
    """)

    display_sql_with_syntax(query)


def demo_subqueries() -> None:
    """Demonstrate subqueries and EXISTS."""
    high_value_customers = sql.select("user_id").from_("orders").group_by("user_id").having("SUM(total_amount) > 10000")

    query = (
        sql.select("id", "name", "email", "department")
        .from_("users")
        .where_exists(high_value_customers.where("orders.user_id = users.id"))
        .where("active = TRUE")
        .order_by("name")
    )
    display_sql_with_syntax(query)


@cli.command()
def interactive() -> None:
    """Launch interactive mode for exploring SQLSpec features."""
    console.print(
        Panel(
            "[bold purple]Interactive SQLSpec Explorer[/bold purple]\nBuild and test SQL queries interactively",
            border_style="purple",
        )
    )

    # Create database
    with console.status("[bold green]Setting up demo database..."):
        db_config = create_sample_database()

    console.print("[green]Database ready! Available tables: users, products, orders[/green]")
    console.print("[yellow]Type 'help' for commands, 'exit' to quit[/yellow]\n")

    while True:
        try:
            user_input = console.input("[bold blue]sqlspec>[/bold blue] ").strip()

            if user_input.lower() in ("exit", "quit"):
                break
            elif user_input.lower() == "help":
                show_interactive_help()
            elif user_input.lower() == "examples":
                show_interactive_examples()
            elif user_input.startswith("sql."):
                try:
                    # Safe eval of sql builder expressions
                    if any(dangerous in user_input for dangerous in ("import", "exec", "eval", "__")):
                        console.print("[red]Invalid command[/red]")
                        continue

                    # Create a safe namespace for evaluation
                    safe_globals = {
                        "sql": sql,
                        "SQL": SQL,
                        "LimitOffsetFilter": LimitOffsetFilter,
                        "SearchFilter": SearchFilter,
                        "OrderByFilter": OrderByFilter,
                    }

                    query = eval(user_input, {"__builtins__": {}}, safe_globals)

                    if hasattr(query, "to_statement"):
                        sql_obj = query.to_statement()
                    elif isinstance(query, SQL):
                        sql_obj = query
                    else:
                        sql_obj = SQL(str(query))

                    display_sql_with_syntax(sql_obj)

                    # Try to execute if it's a SELECT
                    if str(sql_obj).strip().upper().startswith("SELECT"):
                        try:
                            with db_config.provide_session() as driver:
                                result = driver.execute(sql_obj)
                                if result.data:
                                    console.print(f"[green]Returned {len(result.data)} rows[/green]")
                                    if len(result.data) <= MAX_ROWS_TO_DISPLAY:
                                        for row in result.data:
                                            console.print(f"  {row}")
                                    else:
                                        console.print("  First 3 rows:")
                                        for row in result.data[:3]:
                                            console.print(f"    {row}")
                                        console.print(f"  ... and {len(result.data) - 3} more")
                        except Exception as e:
                            console.print(f"[yellow]Query built successfully but execution failed: {e}[/yellow]")

                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
            else:
                console.print("[yellow]Commands must start with 'sql.' - try 'examples' for inspiration[/yellow]")

        except KeyboardInterrupt:
            break
        except EOFError:
            break

    console.print("\n[cyan]Thanks for exploring SQLSpec![/cyan]")


def show_interactive_help() -> None:
    """Show help for interactive mode."""
    help_text = """
[bold cyan]Interactive Commands:[/bold cyan]

• [green]examples[/green] - Show example queries
• [green]help[/green] - Show this help
• [green]exit[/green] - Exit interactive mode

[bold cyan]Query Building:[/bold cyan]

Start with [green]sql.[/green] to build queries:
• [yellow]sql.select("*").from_("users")[/yellow]
• [yellow]sql.insert("users").values(...)[/yellow]
• [yellow]SQL("SELECT * FROM users WHERE id = ?", parameters=[1])[/yellow]

[bold cyan]Available Objects:[/bold cyan]
• sql - Query builder factory object
• SQL - Raw SQL execution
• Filters: LimitOffsetFilter, SearchFilter, OrderByFilter
"""
    console.print(Panel(help_text, title="Help", border_style="blue"))


def show_interactive_examples() -> None:
    """Show example queries for interactive mode."""
    examples = [
        "sql.select('*').from_('users').limit(5)",
        "sql.select('name', 'salary').from_('users').where('salary > 75000')",
        "sql.select('department', 'COUNT(*) as count').from_('users').group_by('department')",
        "sql.select('u.name', 'o.total_amount').from_('users u').inner_join('orders o', 'u.id = o.user_id')",
        "sql.select('*').from_('users').append_filter(LimitOffsetFilter(10, 0))",
    ]

    console.print("[bold cyan]Example Queries:[/bold cyan]\n")
    for i, example in enumerate(examples, 1):
        console.print(f"{i}. [yellow]{example}[/yellow]")
    console.print()


if __name__ == "__main__":
    cli()
