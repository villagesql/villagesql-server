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

    echo ""
    echo "✓ Database initialized successfully!"
    echo ""
    echo "Next steps:"
    echo "  1. Start the server: ./villagesql${DIR_ARG} start"
    echo "  2. Connect to it: ./villagesql${DIR_ARG} connect"
}

cmd_start() {
    set -e

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
        --log-error-verbosity=3 &

    local server_pid=$!
    echo "Server starting (PID: $server_pid)..."

    for i in {1..30}; do
        if "$BASEDIR/bin/mysqladmin" --socket="$socket" ping >/dev/null 2>&1; then
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

    exec "$BASEDIR/bin/mysql" --socket="$socket" -u root "$@"
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

    echo "Stopping VillageSQL server (PID: $pid)..."
    "$BASEDIR/bin/mysqladmin" --socket="$socket" -u root shutdown 2>/dev/null || true

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
    echo "  init        Initialize the database (run once before first use)"
    echo "  start       Start the VillageSQL server"
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
    echo "  ./villagesql --dir /tmp/test-instance init"
    echo "  ./villagesql --dir /tmp/test-instance start"
    echo "  MYSQL_PORT=3308 ./villagesql --dir ./second start"
}

# Build a DIR_ARG string for use in user-facing messages so they can copy-paste
# the exact command needed for their instance.
case "$DIR_FLAG" in
    --here) DIR_ARG=" --here" ;;
    --dir)  DIR_ARG=" --dir $DIR" ;;
    *)      DIR_ARG="" ;;
esac

case "$1" in
    init)    cmd_init ;;
    start)   cmd_start ;;
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
