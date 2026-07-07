#!/bin/bash
# VillageSQL development server control script

BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default instance directory. Override with --dir <path> to run multiple
# independent instances.
DIR="$BASEDIR/instances/default"
DIR_FLAG=""  # tracks which flag was used: "", "--here", or "--dir"

# Parse global flags before the sub-command
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir)
            [[ -z "$2" ]] && { echo "Error: --dir requires a path"; exit 1; }
            DIR="$2"
            DIR_FLAG="--dir"
            shift 2
            ;;
        --dir=*)
            DIR="${1#--dir=}"
            DIR_FLAG="--dir"
            shift
            ;;
        --here)
            DIR="$(pwd)/.vsql_here"
            DIR_FLAG="--here"
            shift
            ;;
        *)
            break
            ;;
    esac
done

cmd_init() {
    set -e

    # Parse init-specific flags
    local password=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --password)
                if [ ! -t 0 ]; then
                    read -r password
                else
                    read -r -s -p "Password for root: " password
                    echo
                    local confirm
                    read -r -s -p "Confirm password: " confirm
                    echo
                    if [[ "$password" != "$confirm" ]]; then
                        echo "Error: Passwords do not match"
                        exit 1
                    fi
                fi
                shift
                ;;
            *)
                echo "Error: Unknown init option: $1"
                exit 1
                ;;
        esac
    done

    # Create the instance directory if it doesn't exist yet
    mkdir -p "$DIR"

    # Resolve to absolute path now that we know it exists
    DIR="$(cd "$DIR" && pwd)"

    local datadir="$DIR/data"

    if [[ -d "$datadir" ]]; then
        echo "Error: Data directory already exists at $datadir"
        echo "Remove it first if you want to reinitialize: rm -rf $datadir"
        exit 1
    fi

    echo "Initializing VillageSQL database..."
    echo "Instance directory: $DIR"
    echo "Data directory: $datadir"

    # Create veb directory and seed it with bundled example extensions
    mkdir -p "$DIR/veb"
    for veb in "$BASEDIR/lib/veb/"*.veb; do
        [[ -e "$veb" ]] && cp "$veb" "$DIR/veb/"
    done

    "$BASEDIR/bin/mysqld" \
        --no-defaults \
        --initialize-insecure \
        --basedir="$BASEDIR" \
        --datadir="$datadir" \
        --log-error-verbosity=3

    if [[ -n "$password" ]]; then
        # Write SQL to set the root password; applied by cmd_start on first boot.
        # Use a subshell with umask 077 so the file is created 600 from the start.
        local escaped="${password//\'/\'\'}"
        (umask 077; printf "ALTER USER 'root'@'localhost' IDENTIFIED BY '%s';\n" "$escaped" > "$DIR/init_password.sql")

        # Write per-instance client config so connect/stop pick up credentials automatically.
        (umask 077; printf '[client]\nuser=root\npassword=%s\n' "$password" > "$DIR/my.cnf")
    fi

    echo ""
    echo "✓ Database initialized successfully!"
    echo ""
    echo "Next steps:"
    echo "  1. Start the server: ./villagesql${DIR_ARG} start"
    echo "  2. Connect to it: ./villagesql${DIR_ARG} connect"
}

cmd_start() {
    set -e

    # Parse start-specific flags. Everything after `--` is forwarded verbatim
    # to mysqld so users can enable/disable features for local testing.
    local extra_mysqld_args=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --)
                shift
                extra_mysqld_args=("$@")
                break
                ;;
            *)
                echo "Error: Unknown start option: $1"
                echo "Use '--' to pass arbitrary flags to mysqld:"
                echo "  ./villagesql${DIR_ARG} start -- --skip-name-resolve"
                exit 1
                ;;
        esac
    done

    if [[ ! -d "$DIR" ]]; then
        echo "Error: Instance directory not found at $DIR"
        echo "Run './villagesql${DIR_ARG} init' first."
        exit 1
    fi

    DIR="$(cd "$DIR" && pwd)"

    local datadir="$DIR/data"
    local vebdir="$DIR/veb"
    local socket="$DIR/mysql.sock"
    local port="${MYSQL_PORT:-3307}"
    local pidfile="$DIR/mysql.pid"
    local logfile="$datadir/error.log"

    if [[ ! -d "$datadir" ]]; then
        echo "Error: Database not initialized. Run './villagesql${DIR_ARG} init' first."
        exit 1
    fi

    if [[ -f "$pidfile" ]]; then
        if kill -0 $(cat "$pidfile") 2>/dev/null; then
            echo "Error: Server is already running (PID: $(cat "$pidfile"))"
            exit 1
        else
            echo "Removing stale PID file..."
            rm -f "$pidfile"
        fi
    fi

    echo "Starting VillageSQL server..."
    echo "  Instance directory: $DIR"
    echo "  Data directory: $datadir"
    echo "  VEB directory: $vebdir"
    echo "  Socket: $socket"
    echo "  Port: $port"
    echo "  Log: $logfile"
    echo ""

    local init_file_args=()
    if [[ -f "$DIR/init_password.sql" ]]; then
        init_file_args=("--init-file=$DIR/init_password.sql")
    fi

    "$BASEDIR/bin/mysqld" \
        --no-defaults \
        --basedir="$BASEDIR" \
        --datadir="$datadir" \
        --veb-dir="$vebdir" \
        --socket="$socket" \
        --port="$port" \
        --bind-address=127.0.0.1 \
        --pid-file="$pidfile" \
        --log-error="$logfile" \
        --log-error-verbosity=3 \
        "${init_file_args[@]}" \
        "${extra_mysqld_args[@]}" &

    local server_pid=$!
    echo "Server starting (PID: $server_pid)..."

    for i in {1..30}; do
        if "$BASEDIR/bin/mysqladmin" --socket="$socket" ping >/dev/null 2>&1; then
            rm -f "$DIR/init_password.sql"
            echo ""
            echo "✓ Server is ready!"
            echo ""
            echo "Connect with: ./villagesql${DIR_ARG} connect"
            echo "Stop with: ./villagesql${DIR_ARG} stop"
            echo "View logs: tail -f $logfile"
            exit 0
        fi
        sleep 1
    done

    echo ""
    echo "Warning: Server did not become ready within 30 seconds."
    echo "Check the log file: $logfile"
    exit 1
}

cmd_connect() {
    if [[ ! -d "$DIR" ]]; then
        echo "Error: Instance directory not found at $DIR"
        exit 1
    fi

    DIR="$(cd "$DIR" && pwd)"

    local socket="$DIR/mysql.sock"

    if [[ ! -S "$socket" ]]; then
        echo "Error: Server doesn't appear to be running (socket not found: $socket)"
        echo "Start it with: ./villagesql${DIR_ARG} start"
        exit 1
    fi

    local auth_args=(-u root)
    [[ -f "$DIR/my.cnf" ]] && auth_args=("--defaults-file=$DIR/my.cnf")

    exec "$BASEDIR/bin/mysql" "${auth_args[@]}" --socket="$socket" "$@"
}

cmd_status() {
    if [[ ! -d "$DIR" ]]; then
        echo "VillageSQL server: no instance directory at $DIR"
        exit 1
    fi

    DIR="$(cd "$DIR" && pwd)"

    local pidfile="$DIR/mysql.pid"
    local socket="$DIR/mysql.sock"

    if [[ ! -f "$pidfile" ]]; then
        echo "VillageSQL server: not running (no PID file)"
        exit 1
    fi

    local pid
    pid=$(cat "$pidfile")

    if ! kill -0 "$pid" 2>/dev/null; then
        echo "VillageSQL server: not running (stale PID file, PID: $pid)"
        exit 1
    fi

    if "$BASEDIR/bin/mysqladmin" --socket="$socket" ping >/dev/null 2>&1; then
        echo "VillageSQL server: running (PID: $pid, socket: $socket)"
        exit 0
    else
        echo "VillageSQL server: process running (PID: $pid) but not accepting connections yet"
        exit 1
    fi
}

cmd_stop() {
    if [[ ! -d "$DIR" ]]; then
        echo "Error: Instance directory not found at $DIR"
        exit 1
    fi

    DIR="$(cd "$DIR" && pwd)"

    local socket="$DIR/mysql.sock"
    local pidfile="$DIR/mysql.pid"

    if [[ ! -f "$pidfile" ]]; then
        echo "Error: PID file not found. Is the server running?"
        exit 1
    fi

    local pid
    pid=$(cat "$pidfile")

    if ! kill -0 "$pid" 2>/dev/null; then
        echo "Error: Server not running (stale PID file)"
        rm -f "$pidfile"
        exit 1
    fi

    local auth_args=(-u root)
    [[ -f "$DIR/my.cnf" ]] && auth_args=("--defaults-file=$DIR/my.cnf")

    echo "Stopping VillageSQL server (PID: $pid)..."
    "$BASEDIR/bin/mysqladmin" "${auth_args[@]}" --socket="$socket" shutdown 2>/dev/null || true

    for i in {1..30}; do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "✓ Server stopped successfully."
            rm -f "$pidfile" "$socket"
            exit 0
        fi
        sleep 1
    done

    echo "Warning: Server did not stop gracefully. Sending SIGTERM..."
    kill "$pid" 2>/dev/null || true
    sleep 2

    if kill -0 "$pid" 2>/dev/null; then
        echo "Warning: Server still running. Sending SIGKILL..."
        kill -9 "$pid" 2>/dev/null || true
    fi

    rm -f "$pidfile" "$socket"
    echo "Server stopped."
}

cmd_veb() {
    local vebdir="$DIR/veb"

    case "$1" in
        ls)
            if [[ ! -d "$vebdir" ]]; then
                echo "No veb directory found at $vebdir"
                echo "Run './villagesql${DIR_ARG} init' first."
                exit 1
            fi
            local found=0
            for veb in "$vebdir/"*.veb; do
                if [[ -e "$veb" ]]; then
                    echo "$(basename "$veb" .veb)"
                    found=1
                fi
            done
            if [[ $found -eq 0 ]]; then
                echo "(no extensions installed)"
            fi
            ;;
        add)
            local src="$2"
            if [[ -z "$src" ]]; then
                echo "Error: veb add requires a path to a .veb file"
                exit 1
            fi
            if [[ ! -f "$src" ]]; then
                echo "Error: file not found: $src"
                exit 1
            fi
            mkdir -p "$vebdir"
            cp "$src" "$vebdir/"
            echo "✓ Added $(basename "$src") to $vebdir"
            ;;
        rm)
            local name="$2"
            if [[ -z "$name" ]]; then
                echo "Error: veb rm requires an extension name"
                exit 1
            fi
            # Accept either "foo" or "foo.veb"
            name="${name%.veb}"
            local target="$vebdir/$name.veb"
            if [[ ! -f "$target" ]]; then
                echo "Error: extension not found: $target"
                exit 1
            fi
            rm "$target"
            echo "✓ Removed $name"
            ;;
        *)
            if [[ -n "$1" ]]; then
                echo "Error: Unknown veb sub-command '$1'"
                echo ""
            fi
            echo "Usage: villagesql${DIR_ARG} veb <sub-command>"
            echo ""
            echo "Sub-commands:"
            echo "  ls        List installed extensions"
            echo "  add <file>  Install a .veb file into this instance"
            echo "  rm <name>   Remove an extension by name"
            exit 1
            ;;
    esac
}

cmd_mysql_test() {
    local mtr="$BASEDIR/mysql-test/mysql-test-run.pl"

    if [[ ! -f "$mtr" ]]; then
        echo "Error: mysql-test-run.pl not found at $mtr"
        exit 1
    fi

    cd "$BASEDIR/mysql-test"
    exec perl mysql-test-run.pl "$@"
}

cmd_help() {
    echo "Usage: villagesql [--dir <path>] <command> [args]"
    echo ""
    echo "Commands:"
    echo "  init [--password]  Initialize the database (run once before first use)"
    echo "  start [-- <mysqld args>...]"
    echo "              Start the VillageSQL server. Args after '--' are passed to mysqld."
    echo "  connect     Connect to the running server (extra args passed to mysql)"
    echo "  stop        Stop the VillageSQL server"
    echo "  status      Check whether the server is running"
    echo "  veb         Manage extensions (ls, add, rm)"
    echo "  mysql-test  Run the test suite (args passed to mysql-test-run.pl)"
    echo "  help        Show this help message"
    echo ""
    echo "Options:"
    echo "  --dir <path>  Instance directory for runtime files (data, socket, PID)."
    echo "                Defaults to instances/default/ within the package directory."
    echo "                Use different paths to run multiple independent instances."
    echo "  --here        Create/use a VillageSQL instance in the current directory"
    echo "                (shorthand for --dir \$(pwd)/.vsql_here)"
    echo ""
    echo "Environment variables:"
    echo "  MYSQL_PORT  Port to use for start (default: 3307)"
    echo ""
    echo "Examples:"
    echo "  ./villagesql init && ./villagesql start"
    echo "  ./villagesql init --password && ./villagesql start"
    echo "  ./villagesql --dir /tmp/test-instance init"
    echo "  ./villagesql --dir /tmp/test-instance start"
    echo "  MYSQL_PORT=3308 ./villagesql --dir ./second start"
    echo "  ./villagesql start -- --skip-name-resolve --general-log"
}

# Build a DIR_ARG string for use in user-facing messages so they can copy-paste
# the exact command needed for their instance.
case "$DIR_FLAG" in
    --here) DIR_ARG=" --here" ;;
    --dir)  DIR_ARG=" --dir $DIR" ;;
    *)      DIR_ARG="" ;;
esac

case "$1" in
    init)    shift; cmd_init "$@" ;;
    start)   shift; cmd_start "$@" ;;
    connect) shift; cmd_connect "$@" ;;
    stop)    cmd_stop ;;
    status)  cmd_status ;;
    veb)        shift; cmd_veb "$@" ;;
    mysql-test) shift; cmd_mysql_test "$@" ;;
    help|--help|-h) cmd_help ;;
    *)
        if [[ -n "$1" ]]; then
            echo "Error: Unknown command '$1'"
            echo ""
        fi
        cmd_help
        exit 1
        ;;
esac
