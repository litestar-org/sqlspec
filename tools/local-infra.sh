#!/usr/bin/env bash

# =============================================================================
# SQLSpec Development Infrastructure Setup
# =============================================================================
#
# A comprehensive script to start and manage development database containers
# with automatic Docker/Podman detection and non-standard ports.
#
# Author: SQLSpec Development Team
# License: MIT
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration and Constants
# -----------------------------------------------------------------------------

readonly SCRIPT_NAME="$(basename "$0")"
readonly VERSION="1.0.0"

# Colors and formatting
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly PURPLE='\033[0;35m'
readonly CYAN='\033[0;36m'
readonly WHITE='\033[1;37m'
readonly BOLD='\033[1m'
readonly NC='\033[0m' # No Color

# Icons
readonly CHECK="✓"
readonly CROSS="✗"
readonly INFO="ℹ"
readonly WARN="⚠"
readonly ROCKET="🚀"
readonly DATABASE="🗄️"
readonly CLOUD="☁️"

# Development ports (non-standard to avoid conflicts)
readonly DEV_POSTGRES_PORT=5433
readonly DEV_ORACLE_PORT=1522
readonly DEV_MYSQL_PORT=3307
readonly DEV_BIGQUERY_PORT=9050
readonly DEV_MINIO_PORT=9001
readonly DEV_MINIO_CONSOLE_PORT=9002

# Container names
readonly POSTGRES_CONTAINER="sqlspec-dev-postgres"
readonly ORACLE_CONTAINER="sqlspec-dev-oracle"
readonly MYSQL_CONTAINER="sqlspec-dev-mysql"
readonly BIGQUERY_CONTAINER="sqlspec-dev-bigquery"
readonly MINIO_CONTAINER="sqlspec-dev-minio"

# Images
readonly POSTGRES_IMAGE="postgres:16-alpine"
readonly ORACLE_IMAGE="gvenzl/oracle-free:23-slim-faststart"
readonly MYSQL_IMAGE="mysql:8.0"
readonly BIGQUERY_IMAGE="ghcr.io/goccy/bigquery-emulator:latest"
readonly MINIO_IMAGE="minio/minio:latest"

# Global variables
CONTAINER_ENGINE=""
SERVICES=()
QUIET_MODE=false
FORCE_RECREATE=false

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------

print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}════════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${WHITE}  ${DATABASE} SQLSpec Development Infrastructure Setup v${VERSION} ${DATABASE}${NC}"
    echo -e "${BOLD}${BLUE}════════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_banner() {
    local message="$1"
    echo ""
    echo -e "${BOLD}${CYAN}────────────────────────────────────────────────────────────────────────────────${NC}"
    echo -e "${BOLD}${WHITE}  ${message}${NC}"
    echo -e "${BOLD}${CYAN}────────────────────────────────────────────────────────────────────────────────${NC}"
    echo ""
}

log_info() {
    [[ "$QUIET_MODE" == "true" ]] && return
    echo -e "${BLUE}${INFO}${NC} $1"
}

log_success() {
    echo -e "${GREEN}${CHECK}${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}${WARN}${NC} $1"
}

log_error() {
    echo -e "${RED}${CROSS}${NC} $1" >&2
}

log_rocket() {
    echo -e "${PURPLE}${ROCKET}${NC} $1"
}

log_database() {
    echo -e "${CYAN}${DATABASE}${NC} $1"
}

show_usage() {
    cat << EOF
${BOLD}USAGE:${NC}
    ${SCRIPT_NAME} <COMMAND> [OPTIONS] [SERVICES...]

${BOLD}COMMANDS:${NC}
    up          Start development infrastructure
    down        Stop development infrastructure
    status      Show status of all containers
    list        List available services
    cleanup     Remove all containers and volumes

${BOLD}SERVICES:${NC}
    postgres    PostgreSQL database (port ${DEV_POSTGRES_PORT})
    oracle      Oracle Free database (port ${DEV_ORACLE_PORT})
    mysql       MySQL database (port ${DEV_MYSQL_PORT})
    bigquery    BigQuery emulator (port ${DEV_BIGQUERY_PORT})
    minio       MinIO cloud storage (port ${DEV_MINIO_PORT})
    all         All services (default)

${BOLD}OPTIONS:${NC}
    -h, --help          Show this help message
    -v, --version       Show version information
    -q, --quiet         Quiet mode (minimal output)
    -f, --force         Force recreate containers (up command only)

${BOLD}EXAMPLES:${NC}
    ${SCRIPT_NAME} up                       # Start all services
    ${SCRIPT_NAME} up postgres mysql        # Start only PostgreSQL and MySQL
    ${SCRIPT_NAME} up --force               # Force recreate all containers
    ${SCRIPT_NAME} down                     # Stop all services
    ${SCRIPT_NAME} down postgres            # Stop only PostgreSQL
    ${SCRIPT_NAME} status                   # Show container status
    ${SCRIPT_NAME} cleanup                  # Clean up everything

${BOLD}PORTS:${NC}
    PostgreSQL: ${DEV_POSTGRES_PORT}    Oracle: ${DEV_ORACLE_PORT}    MySQL: ${DEV_MYSQL_PORT}
    BigQuery:   ${DEV_BIGQUERY_PORT}    MinIO:  ${DEV_MINIO_PORT}

${BOLD}CONNECTION EXAMPLES:${NC}
    PostgreSQL: postgresql://postgres:postgres@localhost:${DEV_POSTGRES_PORT}/postgres
    Oracle:     oracle://system:oracle@localhost:${DEV_ORACLE_PORT}/FREEPDB1
    MySQL:      mysql://root:mysql@localhost:${DEV_MYSQL_PORT}/test
    MinIO:      http://localhost:${DEV_MINIO_PORT} (admin:password123)

EOF
}

show_version() {
    echo "${SCRIPT_NAME} version ${VERSION}"
}

# -----------------------------------------------------------------------------
# Container Engine Detection
# -----------------------------------------------------------------------------

detect_container_engine() {
    log_info "Detecting container engine..."

    if command -v podman >/dev/null 2>&1; then
        CONTAINER_ENGINE="podman"
        log_success "Found Podman container engine"
    elif command -v docker >/dev/null 2>&1; then
        CONTAINER_ENGINE="docker"
        log_success "Found Docker container engine"
    else
        log_error "Neither Docker nor Podman found. Please install one of them."
        exit 1
    fi

    # Test if engine is actually working
    if ! $CONTAINER_ENGINE info >/dev/null 2>&1; then
        log_error "${CONTAINER_ENGINE} is installed but not running or accessible"
        log_info "Try: sudo systemctl start ${CONTAINER_ENGINE}"
        exit 1
    fi
}

# -----------------------------------------------------------------------------
# Port Management
# -----------------------------------------------------------------------------

check_port() {
    local port=$1
    if netstat -tuln 2>/dev/null | grep -q ":${port} " || \
       ss -tuln 2>/dev/null | grep -q ":${port} "; then
        return 1  # Port is in use
    fi
    return 0  # Port is free
}

wait_for_port() {
    local host=$1
    local port=$2
    local service=$3
    local timeout=${4:-30}

    log_info "Waiting for ${service} to be ready on port ${port}..."

    for ((i=1; i<=timeout; i++)); do
        if nc -z "$host" "$port" 2>/dev/null; then
            log_success "${service} is ready!"
            return 0
        fi
        sleep 1
    done

    log_warn "${service} is not responding after ${timeout} seconds"
    return 1
}

# -----------------------------------------------------------------------------
# Container Management
# -----------------------------------------------------------------------------

container_exists() {
    local name=$1
    $CONTAINER_ENGINE ps -a --format "{{.Names}}" | grep -q "^${name}$"
}

container_running() {
    local name=$1
    $CONTAINER_ENGINE ps --format "{{.Names}}" | grep -q "^${name}$"
}

stop_container() {
    local name=$1
    if container_running "$name"; then
        log_info "Stopping container: $name"
        $CONTAINER_ENGINE stop "$name" >/dev/null
        log_success "Stopped: $name"
    fi
}

remove_container() {
    local name=$1
    if container_exists "$name"; then
        stop_container "$name"
        log_info "Removing container: $name"
        $CONTAINER_ENGINE rm "$name" >/dev/null
        log_success "Removed: $name"
    fi
}

# -----------------------------------------------------------------------------
# Service Implementations
# -----------------------------------------------------------------------------

start_postgres() {
    local container_name="$POSTGRES_CONTAINER"
    log_database "Starting PostgreSQL..."

    if container_running "$container_name"; then
        log_success "PostgreSQL is already running"
        return 0
    fi

    if ! check_port $DEV_POSTGRES_PORT; then
        log_error "Port $DEV_POSTGRES_PORT is already in use"
        return 1
    fi

    [[ "$FORCE_RECREATE" == "true" ]] && remove_container "$container_name"

    if ! container_exists "$container_name"; then
        log_info "Creating PostgreSQL container..."
        $CONTAINER_ENGINE run -d \
            --name "$container_name" \
            -p "${DEV_POSTGRES_PORT}:5432" \
            -e POSTGRES_USER=postgres \
            -e POSTGRES_PASSWORD=postgres \
            -e POSTGRES_DB=postgres \
            -e POSTGRES_INITDB_ARGS="--auth-host=scram-sha-256" \
            --tmpfs /var/lib/postgresql/data:noexec,nosuid,size=1G \
            "$POSTGRES_IMAGE" >/dev/null
    else
        log_info "Starting existing PostgreSQL container..."
        $CONTAINER_ENGINE start "$container_name" >/dev/null
    fi

    wait_for_port localhost $DEV_POSTGRES_PORT "PostgreSQL"
    log_success "PostgreSQL ready on port $DEV_POSTGRES_PORT"
    echo -e "  ${BOLD}Connection:${NC} postgresql://postgres:postgres@localhost:${DEV_POSTGRES_PORT}/postgres"
}

start_oracle() {
    local container_name="$ORACLE_CONTAINER"
    log_database "Starting Oracle..."

    if container_running "$container_name"; then
        log_success "Oracle is already running"
        return 0
    fi

    if ! check_port $DEV_ORACLE_PORT; then
        log_error "Port $DEV_ORACLE_PORT is already in use"
        return 1
    fi

    [[ "$FORCE_RECREATE" == "true" ]] && remove_container "$container_name"

    if ! container_exists "$container_name"; then
        log_info "Creating Oracle container (this may take a while)..."
        $CONTAINER_ENGINE run -d \
            --name "$container_name" \
            -p "${DEV_ORACLE_PORT}:1521" \
            -e ORACLE_PASSWORD=oracle \
            -e ORACLE_DATABASE=FREEPDB1 \
            --tmpfs /opt/oracle/oradata:noexec,nosuid,size=2G \
            "$ORACLE_IMAGE" >/dev/null
    else
        log_info "Starting existing Oracle container..."
        $CONTAINER_ENGINE start "$container_name" >/dev/null
    fi

    wait_for_port localhost $DEV_ORACLE_PORT "Oracle" 60
    log_success "Oracle ready on port $DEV_ORACLE_PORT"
    echo -e "  ${BOLD}Connection:${NC} oracle://system:oracle@localhost:${DEV_ORACLE_PORT}/FREEPDB1"
}

start_mysql() {
    local container_name="$MYSQL_CONTAINER"
    log_database "Starting MySQL..."

    if container_running "$container_name"; then
        log_success "MySQL is already running"
        return 0
    fi

    if ! check_port $DEV_MYSQL_PORT; then
        log_error "Port $DEV_MYSQL_PORT is already in use"
        return 1
    fi

    [[ "$FORCE_RECREATE" == "true" ]] && remove_container "$container_name"

    if ! container_exists "$container_name"; then
        log_info "Creating MySQL container..."
        $CONTAINER_ENGINE run -d \
            --name "$container_name" \
            -p "${DEV_MYSQL_PORT}:3306" \
            -e MYSQL_ROOT_PASSWORD=mysql \
            -e MYSQL_DATABASE=test \
            -e MYSQL_USER=user \
            -e MYSQL_PASSWORD=password \
            --tmpfs /var/lib/mysql:noexec,nosuid,size=1G \
            "$MYSQL_IMAGE" >/dev/null
    else
        log_info "Starting existing MySQL container..."
        $CONTAINER_ENGINE start "$container_name" >/dev/null
    fi

    wait_for_port localhost $DEV_MYSQL_PORT "MySQL"
    log_success "MySQL ready on port $DEV_MYSQL_PORT"
    echo -e "  ${BOLD}Connection:${NC} mysql://root:mysql@localhost:${DEV_MYSQL_PORT}/test"
}

start_bigquery() {
    local container_name="$BIGQUERY_CONTAINER"
    log_database "Starting BigQuery Emulator..."

    if container_running "$container_name"; then
        log_success "BigQuery Emulator is already running"
        return 0
    fi

    if ! check_port $DEV_BIGQUERY_PORT; then
        log_error "Port $DEV_BIGQUERY_PORT is already in use"
        return 1
    fi

    [[ "$FORCE_RECREATE" == "true" ]] && remove_container "$container_name"

    if ! container_exists "$container_name"; then
        log_info "Creating BigQuery Emulator container..."
        $CONTAINER_ENGINE run -d \
            --name "$container_name" \
            -p "${DEV_BIGQUERY_PORT}:9050" \
            -e PROJECT_ID=test-project \
            "$BIGQUERY_IMAGE" >/dev/null
    else
        log_info "Starting existing BigQuery Emulator container..."
        $CONTAINER_ENGINE start "$container_name" >/dev/null
    fi

    wait_for_port localhost $DEV_BIGQUERY_PORT "BigQuery Emulator"
    log_success "BigQuery Emulator ready on port $DEV_BIGQUERY_PORT"
    echo -e "  ${BOLD}Endpoint:${NC} http://localhost:${DEV_BIGQUERY_PORT}"
}

start_minio() {
    local container_name="$MINIO_CONTAINER"
    log_database "Starting MinIO..."

    if container_running "$container_name"; then
        log_success "MinIO is already running"
        return 0
    fi

    if ! check_port $DEV_MINIO_PORT || ! check_port $DEV_MINIO_CONSOLE_PORT; then
        log_error "MinIO ports ($DEV_MINIO_PORT, $DEV_MINIO_CONSOLE_PORT) are in use"
        return 1
    fi

    [[ "$FORCE_RECREATE" == "true" ]] && remove_container "$container_name"

    if ! container_exists "$container_name"; then
        log_info "Creating MinIO container..."
        $CONTAINER_ENGINE run -d \
            --name "$container_name" \
            -p "${DEV_MINIO_PORT}:9000" \
            -p "${DEV_MINIO_CONSOLE_PORT}:9001" \
            -e MINIO_ROOT_USER=admin \
            -e MINIO_ROOT_PASSWORD=password123 \
            --tmpfs /data:noexec,nosuid,size=1G \
            "$MINIO_IMAGE" server /data --console-address ":9001" >/dev/null
    else
        log_info "Starting existing MinIO container..."
        $CONTAINER_ENGINE start "$container_name" >/dev/null
    fi

    wait_for_port localhost $DEV_MINIO_PORT "MinIO"
    log_success "MinIO ready on ports $DEV_MINIO_PORT (API) and $DEV_MINIO_CONSOLE_PORT (Console)"
    echo -e "  ${BOLD}API:${NC}     http://localhost:$DEV_MINIO_PORT"
    echo -e "  ${BOLD}Console:${NC} http://localhost:$DEV_MINIO_CONSOLE_PORT (admin:password123)"
}

# -----------------------------------------------------------------------------
# Status and Management Functions
# -----------------------------------------------------------------------------

show_status() {
    print_banner "Container Status"

    local containers=("$POSTGRES_CONTAINER" "$ORACLE_CONTAINER" "$MYSQL_CONTAINER" "$BIGQUERY_CONTAINER" "$MINIO_CONTAINER")
    local services=("PostgreSQL" "Oracle" "MySQL" "BigQuery" "MinIO")
    local ports=("$DEV_POSTGRES_PORT" "$DEV_ORACLE_PORT" "$DEV_MYSQL_PORT" "$DEV_BIGQUERY_PORT" "$DEV_MINIO_PORT")

    for i in "${!containers[@]}"; do
        local container="${containers[$i]}"
        local service="${services[$i]}"
        local port="${ports[$i]}"

        if container_running "$container"; then
            log_success "${service} is running on port ${port}"
        elif container_exists "$container"; then
            log_warn "${service} exists but is stopped"
        else
            echo -e "${BLUE}${INFO}${NC} ${service} is not created"
        fi
    done
}

stop_all() {
    print_banner "Stopping Development Infrastructure"

    local containers=("$POSTGRES_CONTAINER" "$ORACLE_CONTAINER" "$MYSQL_CONTAINER" "$BIGQUERY_CONTAINER" "$MINIO_CONTAINER")

    for container in "${containers[@]}"; do
        stop_container "$container"
    done

    log_success "All development containers stopped"
}

stop_services() {
    local services=("$@")

    if [[ ${#services[@]} -eq 0 ]] || [[ "${services[0]}" == "all" ]]; then
        stop_all
        return
    fi

    print_banner "Stopping Selected Services"

    for service in "${services[@]}"; do
        case $service in
            postgres) stop_container "$POSTGRES_CONTAINER" ;;
            oracle) stop_container "$ORACLE_CONTAINER" ;;
            mysql) stop_container "$MYSQL_CONTAINER" ;;
            bigquery) stop_container "$BIGQUERY_CONTAINER" ;;
            minio) stop_container "$MINIO_CONTAINER" ;;
            *) log_error "Unknown service: $service" ;;
        esac
    done
}

cleanup_all() {
    print_banner "Cleaning Up All Development Containers"

    local containers=("$POSTGRES_CONTAINER" "$ORACLE_CONTAINER" "$MYSQL_CONTAINER" "$BIGQUERY_CONTAINER" "$MINIO_CONTAINER")

    for container in "${containers[@]}"; do
        remove_container "$container"
    done

    log_info "Removing unused volumes..."
    $CONTAINER_ENGINE volume prune -f >/dev/null 2>&1 || true

    log_success "Cleanup complete"
}

list_services() {
    print_banner "Available Services"

    echo -e "${BOLD}Database Services:${NC}"
    echo -e "  postgres    PostgreSQL 16 (port ${DEV_POSTGRES_PORT})"
    echo -e "  oracle      Oracle Free 23c (port ${DEV_ORACLE_PORT})"
    echo -e "  mysql       MySQL 8.0 (port ${DEV_MYSQL_PORT})"
    echo -e "  bigquery    BigQuery Emulator (port ${DEV_BIGQUERY_PORT})"
    echo ""
    echo -e "${BOLD}Storage Services:${NC}"
    echo -e "  minio       MinIO Cloud Storage (ports ${DEV_MINIO_PORT}, ${DEV_MINIO_CONSOLE_PORT})"
    echo ""
    echo -e "${BOLD}Meta Services:${NC}"
    echo -e "  all         Start all services (default)"
}

# -----------------------------------------------------------------------------
# Main Logic
# -----------------------------------------------------------------------------

start_services() {
    local services=("$@")

    # Default to all services if none specified
    if [[ ${#services[@]} -eq 0 ]]; then
        services=("all")
    fi

    # Start services
    print_banner "Starting Development Infrastructure"

    local start_all=false
    for service in "${services[@]}"; do
        if [[ "$service" == "all" ]]; then
            start_all=true
            break
        fi
    done

    if [[ "$start_all" == "true" ]]; then
        log_rocket "Starting all development services..."
        start_postgres
        start_oracle
        start_mysql
        start_bigquery
        start_minio
    else
        for service in "${services[@]}"; do
            case $service in
                postgres) start_postgres ;;
                oracle) start_oracle ;;
                mysql) start_mysql ;;
                bigquery) start_bigquery ;;
                minio) start_minio ;;
                *) log_error "Unknown service: $service" ;;
            esac
        done
    fi

    # Show final status
    echo ""
    log_rocket "Development infrastructure is ready!"
    echo ""
    echo -e "${BOLD}Quick Commands:${NC}"
    echo -e "  ${SCRIPT_NAME} status     Show container status"
    echo -e "  ${SCRIPT_NAME} down       Stop all containers"
    echo -e "  ${SCRIPT_NAME} cleanup    Remove all containers"
    echo ""
}

main() {
    # Check for command
    if [[ $# -eq 0 ]]; then
        log_error "Missing command. Use --help for usage information."
        exit 1
    fi

    local command=""
    local options=()
    local services=()

    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -v|--version)
                show_version
                exit 0
                ;;
            -q|--quiet)
                QUIET_MODE=true
                shift
                ;;
            -f|--force)
                FORCE_RECREATE=true
                shift
                ;;
            up|down|status|list|cleanup)
                if [[ -n "$command" ]]; then
                    log_error "Multiple commands specified: $command and $1"
                    exit 1
                fi
                command="$1"
                shift
                ;;
            postgres|oracle|mysql|bigquery|minio|all)
                services+=("$1")
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                echo "Use --help for usage information."
                exit 1
                ;;
        esac
    done

    # Validate command
    if [[ -z "$command" ]]; then
        log_error "No command specified. Use --help for usage information."
        exit 1
    fi

    # Show header for interactive commands
    if [[ "$command" != "status" && "$command" != "list" && "$QUIET_MODE" != "true" ]]; then
        print_header
    fi

    # Detect container engine for commands that need it
    if [[ "$command" != "list" ]]; then
        detect_container_engine
    fi

    # Execute command
    case $command in
        up)
            start_services "${services[@]}"
            ;;
        down)
            stop_services "${services[@]}"
            ;;
        status)
            show_status
            ;;
        list)
            list_services
            ;;
        cleanup)
            echo -e "${YELLOW}${WARN}${NC} This will remove all development containers and volumes."
            read -r -p "Are you sure? (y/N): " confirm
            if [[ $confirm =~ ^[Yy]$ ]]; then
                cleanup_all
            else
                log_info "Cleanup cancelled"
            fi
            ;;
        *)
            log_error "Unknown command: $command"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
