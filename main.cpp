#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <queue>
#include <chrono>
#include <map>
#include <numeric>
#include <algorithm>
#include <atomic>
#include <iomanip> // For std::setprecision
#include <cstdlib> // For rand(), srand()
#include <ctime>   // For time()
#include <cstdio>  // For printf, fflush
#include <fstream> // For file I/O
#include <random>  // For better random numbers (optional, could stick with rand)
#include <string_view> // For string constants

// spdlog headers
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h" // For colored console output
#include "spdlog/fmt/fmt.h" // Include fmt directly for formatting

// System headers for sockets and kqueue
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>     // For close(), read(), write()
#include <fcntl.h>      // For fcntl() and O_NONBLOCK
#include <sys/event.h>  // For kqueue
#include <sys/time.h>   // For struct timespec (for kevent timeout)
#include <sys/types.h>
#include <cerrno>       // For errno
#include <cstring>      // For memset, strerror

// --- Configuration ---
constexpr int NUM_CONNECTIONS = 500;
const char* SERVER_IP = "127.0.0.1"; // Replace with actual server IP if needed
constexpr int PORT = 5001;             // Replace with actual server port
constexpr int BUFFER_SIZE = 1024;
constexpr int KEVENT_MAX_EVENTS = 64; // Max events to process per kevent call
constexpr int INITIAL_RETRY_DELAY_MS = 1000; // Initial delay before first retry
constexpr int MAX_RETRY_DELAY_MS = 30000;    // Maximum delay between retries
const char* SENTENCE_FILE = "korean_sentences.txt";
const char* RESULT_FILE = "latency_results.txt";
constexpr int MAX_REQUESTS_BEFORE_SHUTDOWN = 10000; // Total requests to send before stopping
constexpr auto KEVENT_TIMEOUT = std::chrono::milliseconds(100); // Timeout for kevent wait
constexpr auto RETRY_CHECK_INTERVAL = std::chrono::milliseconds(100); // How often to check for retries
constexpr auto STATUS_LOG_INTERVAL = std::chrono::seconds(5); // How often to log connection status

// --- Client State ---
enum class ClientStatus {
    DISCONNECTED,
    CONNECTING,
    CONNECTED
};

struct ClientState {
    int socket_fd = -1;
    ClientStatus status = ClientStatus::DISCONNECTED;
    std::chrono::steady_clock::time_point current_request_start_time = std::chrono::steady_clock::time_point::min(); // Time when the current request was added to write buffer
    std::vector<double> latencies_ms;
    std::vector<char> write_buffer; // Buffer for pending writes
    std::vector<char> read_buffer;  // Buffer for partial reads
    int retry_attempts = 0; // Number of consecutive failed connection attempts (0=connected/never failed, >0=failed, -1=stopped retrying)
    std::chrono::steady_clock::time_point next_retry_time; // Time point for the next connection attempt

    ClientState() = default; // Default constructor

    // Prevent copying, allow moving
    ClientState(const ClientState&) = delete;
    ClientState& operator=(const ClientState&) = delete;
    ClientState(ClientState&&) = default;
    ClientState& operator=(ClientState&&) = default;
};

// --- Global Variables ---
std::vector<ClientState> clients(NUM_CONNECTIONS);
std::atomic<bool> running(true);
std::atomic<int> connected_clients_count(0); // Counter for currently connected clients
std::atomic<bool> all_clients_connected{false}; // Flag to indicate if all clients are connected
std::atomic<int> total_requests_initiated{0}; // Counter for total requests initiated across all clients
std::atomic<int> total_responses_received{0}; // Counter for total responses received across all clients
std::atomic<bool> test_timer_started{false};   // Flag to ensure start timer is recorded once
std::chrono::steady_clock::time_point test_start_time;
std::chrono::steady_clock::time_point test_end_time;
int last_logged_count = -1; // Track last logged count for status updates
std::shared_ptr<spdlog::logger> console; // spdlog logger instance
std::vector<std::string> sentences; // Global storage for sentences

// --- Helper Functions ---

// Error logging helper (uses spdlog)
void log_error(const std::string& msg) {
    // Always log error with errno string
    if (console) {
        console->error("{}: {}", msg, strerror(errno));
    } else {
        std::cerr << "ERROR: " << msg << " - " << strerror(errno) << std::endl;
    }
}

// Set socket to non-blocking mode
bool set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        log_error(fmt::format("fcntl(F_GETFL) failed for fd {}", fd));
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        log_error(fmt::format("fcntl(F_SETFL) failed for fd {}", fd));
        return false;
    }
    return true;
}


// Function to log connection status if changed
void log_connection_status_if_changed() {
    int current_count = connected_clients_count.load();
    if (current_count != last_logged_count) {
        if (console) {
            console->info("[Status] Connected Clients: {}/{}", current_count, NUM_CONNECTIONS);
        } else {
            std::cout << "[Status] Connected Clients: " << current_count << "/" << NUM_CONNECTIONS << std::endl;
        }
        last_logged_count = current_count;
    }
}

// --- Forward declarations ---
void handle_write(ClientState* state, int kq);
void handle_read(ClientState* state, int kq);
bool attempt_connection(ClientState* state, int kq, const struct sockaddr_in& serv_addr);
void send_random_sentence(ClientState* state, int kq);
void close_client_connection(ClientState* state, int kq, const std::string& reason); // Keep forward declaration

// --- Kqueue Management ---
// Removes read/write events for a given fd from kqueue.
void remove_kqueue_events(int kq, int fd) {
    if (kq == -1 || fd == -1) return; // Nothing to remove if kq or fd is invalid

    struct kevent changes[2];
    int nchanges = 0;
    // Use udata=nullptr as we don't need it for deletion
    EV_SET(&changes[nchanges++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&changes[nchanges++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);

    if (kevent(kq, changes, nchanges, nullptr, 0, nullptr) == -1) {
        // Ignore errors if fd is already closed (EBADF) or event not found (ENOENT)
        if (errno != ENOENT && errno != EBADF) {
            log_error(fmt::format("kevent EV_DELETE failed for fd {}", fd));
        }
    }
}

// --- Connection Management ---

// Schedules the next retry attempt for a disconnected client.
void schedule_retry(ClientState* state) {
    if (!state || !running.load() || state->retry_attempts == -1) {
        if(state) state->retry_attempts = -1; // Ensure marked as stopped if not running or already stopped
        return;
    }

    state->retry_attempts++;
    int client_idx = state - &clients[0]; // Calculate index for logging

    // Exponential backoff with jitter
    int exponent = std::min(state->retry_attempts - 1, 10); // Limit exponent to avoid large delays/overflow
    int base_delay_ms = INITIAL_RETRY_DELAY_MS * (1 << exponent);
    int delay_ms = std::min(MAX_RETRY_DELAY_MS, base_delay_ms);

    // Add jitter (+/- 10% of the delay)
    int jitter_range = delay_ms / 10;
    int jitter = (jitter_range > 0) ? (rand() % (2 * jitter_range + 1)) - jitter_range : 0;
    delay_ms = std::max(0, delay_ms + jitter); // Ensure non-negative delay

    state->next_retry_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms);

    if (console) {
        console->info("[Conn {}] Scheduling retry attempt {} in approx {} ms.", client_idx, state->retry_attempts, delay_ms);
    } else {
        std::cout << "[Conn " << client_idx << "] Scheduling retry attempt " << state->retry_attempts
                  << " in approx " << delay_ms << " ms." << std::endl;
    }
}


// Close client connection helper
void close_client_connection(ClientState* state, int kq, const std::string& reason) {
    if (!state || state->socket_fd == -1) {
        return; // Already closed or invalid state
    }

    int client_idx = state - &clients[0]; // Calculate index for logging
    int fd_to_close = state->socket_fd; // Store fd before resetting state
    bool was_connected = (state->status == ClientStatus::CONNECTED);

    if (console) {
        console->warn("[Conn {}] Closing connection (fd: {}). Reason: {}", client_idx, fd_to_close, reason);
    } else {
        std::cerr << "\n[Conn " << client_idx << "] Closing connection (fd: " << fd_to_close << "). Reason: " << reason << std::endl;
    }

    // 1. Remove events from kqueue *before* closing fd
    remove_kqueue_events(kq, fd_to_close);

    // 2. Close the socket
    close(fd_to_close);

    // 3. Reset client state
    state->socket_fd = -1;
    state->status = ClientStatus::DISCONNECTED;
    state->write_buffer.clear();
    state->read_buffer.clear();
    state->current_request_start_time = std::chrono::steady_clock::time_point::min(); // Reset request time

    // 4. Update global counter if it was connected
    if (was_connected) {
        connected_clients_count--;
        log_connection_status_if_changed(); // Log status immediately after disconnect
    }

    // 5. Schedule retry (if applicable)
    schedule_retry(state);
}


// --- Input Thread Function ---
void input_thread_func() {
    std::string line;
    while (running.load()) {
        // Prompt on its own line for clarity
        std::cout << "\n[Input] Type 'exit' and press Enter to stop: " << std::flush;

        if (!std::getline(std::cin, line)) {
            if (std::cin.eof()) {
                 if(console) console->info("[Input] EOF detected, signaling exit.");
                 else std::cout << "\n[Input] EOF detected, signaling exit." << std::endl;
                 running = false; // Signal exit on EOF
            } else {
                 // Only log error if running is still true, otherwise it's expected during shutdown
                 if (running.load()) {
                     // Use strerror(errno) as std::getline might set errno
                     if(console) console->error("[Input] Error reading stdin: {}", strerror(errno));
                     else std::cerr << "\n[Input] Error reading stdin: " << strerror(errno) << std::endl;
                 }
                 running = false; // Signal exit on error
            }
            break; // Exit loop on EOF or error
        }

        // Trim whitespace (optional, but good practice)
        line.erase(0, line.find_first_not_of(" \t\n\r"));
        line.erase(line.find_last_not_of(" \t\n\r") + 1);

        if (line == "exit") {
            if(console) console->info("[Input] 'exit' command received, signaling shutdown.");
            else std::cout << "[Input] 'exit' command received, signaling shutdown." << std::endl;
            running = false; // Signal exit
            break; // Exit loop
        }
        // Ignore other input silently
    }
    if(console) console->info("[Input] Input thread exiting.");
    else std::cout << "[Input] Input thread exiting." << std::endl;
}

// --- Connection Attempt Function ---
bool attempt_connection(ClientState* state, int kq, const struct sockaddr_in& serv_addr) {
    int client_idx = state - &clients[0];

    if (state->socket_fd != -1) {
        // This shouldn't happen if logic is correct, but handle defensively
        if(console) console->warn("[WARN][Conn {}] Socket fd {} not -1 when attempting connection. Closing first.", client_idx, state->socket_fd);
        else std::cerr << "[WARN][Conn " << client_idx << "] Socket fd " << state->socket_fd << " not -1 when attempting connection. Closing first." << std::endl;
        // Use the helper to ensure proper cleanup and potential retry scheduling
        close_client_connection(state, kq, "Duplicate connection attempt");
    }
    // Ensure status is reset after potential close_client_connection call
    state->status = ClientStatus::DISCONNECTED;

    // 1. Create socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        log_error(fmt::format("socket() creation failed for connection {}", client_idx));
        // No fd to close yet, just mark state and schedule retry
        schedule_retry(state);
        return false;
    }

    // 2. Set non-blocking
    if (!set_nonblocking(sock)) {
        // Error logged by set_nonblocking
        close(sock); // Close the newly created socket
        schedule_retry(state);
        return false;
    }

    // 3. Update state (pre-connect)
    state->socket_fd = sock;
    state->status = ClientStatus::CONNECTING;
    state->current_request_start_time = std::chrono::steady_clock::time_point::min(); // Reset request time

    // 4. Attempt non-blocking connect
    int ret = connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

    if (ret == 0) {
        // Connected immediately
        if(console) console->info("[Conn {}] Connected immediately (fd: {})", client_idx, sock);
        else std::cout << "\n[Conn " << client_idx << "] Connected immediately (fd: " << sock << ")" << std::endl;

        state->status = ClientStatus::CONNECTED;
        state->retry_attempts = 0; // Reset retry count
        connected_clients_count++;
        log_connection_status_if_changed();

        // Register for read events only (write will be added when needed)
        struct kevent socket_change;
        EV_SET(&socket_change, sock, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, state);
        if (kevent(kq, &socket_change, 1, nullptr, 0, nullptr) == -1) {
            log_error(fmt::format("kevent() failed to register immediate connection read for {}", client_idx));
            close_client_connection(state, kq, "kevent register read failed"); // Cleanup
            return false; // Return false as connection setup failed
        }

        // Check if this connection makes all clients connected
        if (connected_clients_count.load() == NUM_CONNECTIONS && !all_clients_connected.load()) {
             all_clients_connected = true;
             console->info(">>> All {} clients connected! Starting test timer and triggering initial sends (max {} requests). <<<", NUM_CONNECTIONS, MAX_REQUESTS_BEFORE_SHUTDOWN);
             // Start the test timer only once
             if (!test_timer_started.load()) {
                 test_start_time = std::chrono::steady_clock::now();
                 test_timer_started = true;
             }
             // Trigger initial send for all *currently* connected clients
             for (int i = 0; i < NUM_CONNECTIONS; ++i) {
                 if (clients[i].status == ClientStatus::CONNECTED) {
                     send_random_sentence(&clients[i], kq); // send_random_sentence now checks the limit
                 }
             }
        }
        // If all clients were already connected (e.g., a reconnect), just try sending for this one client.
        else if (all_clients_connected.load()) {
             send_random_sentence(state, kq);
        }

        return true; // Success

    } else if (errno == EINPROGRESS) {
        // Connection pending
        state->status = ClientStatus::CONNECTING;
        struct kevent socket_changes[2];
        // Register for WRITE to detect connection completion/failure
        EV_SET(&socket_changes[0], sock, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, state);
        // Register for READ as well (server might send data before connection fully completes?)
        EV_SET(&socket_changes[1], sock, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, state);
        if (kevent(kq, socket_changes, 2, nullptr, 0, nullptr) == -1) {
            log_error(fmt::format("kevent() failed to register pending connection for {}", client_idx));
            close_client_connection(state, kq, "kevent register failed"); // Cleanup
            return false; // Return false as connection setup failed
        }
        // Connection attempt initiated, waiting for EVFILT_WRITE event
        return true; // Indicate attempt is in progress

    } else {
        // Connection failed immediately
        log_error(fmt::format("connect() failed immediately for connection {}", client_idx));
        close(sock); // Close the failed socket fd
        state->socket_fd = -1; // Reset fd in state
        state->status = ClientStatus::DISCONNECTED;
        schedule_retry(state); // Schedule retry
        return false; // Failure
    }
}

// --- Send Random Sentence Function ---
void send_random_sentence(ClientState* state, int kq) {
    // Only send if connected, socket is valid, all clients are connected, AND we haven't reached the request limit.
    if (!state || state->socket_fd == -1 || state->status != ClientStatus::CONNECTED || !all_clients_connected.load()) {
        return;
    }

    // Check if we have initiated enough requests
    if (total_requests_initiated.load() >= MAX_REQUESTS_BEFORE_SHUTDOWN) {
        // Optional: Log that we are stopping sends? Might be too verbose.
        return;
    }

    // Increment initiated requests *before* attempting send
    // Use fetch_add to ensure atomicity and get the value before incrementing
    int current_request_num = total_requests_initiated.fetch_add(1);
    if (current_request_num >= MAX_REQUESTS_BEFORE_SHUTDOWN) {
        // Another thread might have incremented past the limit between the check and fetch_add.
        // Decrement if we went over (though unlikely to happen often with fetch_add).
        total_requests_initiated.fetch_sub(1);
        return;
    }

    int client_idx = state - &clients[0];

    if (sentences.empty()) {
        if (console) console->error("[Conn {}] Cannot send message, sentences list is empty.", client_idx);
        else std::cerr << "[Conn " << client_idx << "] Error: Cannot send message, sentences list is empty." << std::endl;
        // Consider closing connection or handling this differently? For now, just return.
        return;
    }

    // Check if already waiting for a response (start time is set)
    if (state->current_request_start_time != std::chrono::steady_clock::time_point::min()) {
        // This indicates we received a read event and processed a response,
        // but handle_write hasn't finished sending the *previous* request yet.
        // Or, we are calling send_random_sentence too eagerly.
        // For this benchmark, we assume one request/response cycle at a time per client.
        // If a response arrives, we immediately try to send the next.
        // If the write buffer isn't empty, handle_write will eventually send it.
        // If the write buffer *is* empty, this call will add to it and potentially trigger handle_write.
        // Let's proceed, but be mindful of this potential overlap.
        // if(console) console->debug("[Conn {}] Still waiting for previous response or write buffer not empty, queuing next send.", client_idx);
    }

    // Select random sentence
    // Using rand() for simplicity, consider <random> for better distribution if needed
    int index = rand() % sentences.size();
    const std::string& sentence = sentences[index];
    std::string message_to_send = sentence + "\n"; // Ensure newline terminator

    bool needs_write_event = state->write_buffer.empty(); // Check *before* adding

    // Record start time *before* adding to buffer/sending
    state->current_request_start_time = std::chrono::steady_clock::now();

    // Add to buffer
    state->write_buffer.insert(state->write_buffer.end(), message_to_send.begin(), message_to_send.end());

    // Try an immediate send if possible (might complete synchronously)
    // This is an optimization to avoid waiting for the next kqueue event if the socket is ready.
    if (needs_write_event) { // Only try immediate send if buffer was previously empty
        ssize_t bytes_sent = send(state->socket_fd, state->write_buffer.data(), state->write_buffer.size(), 0); // MSG_DONTWAIT could be used, but kqueue handles non-blocking

        if (bytes_sent > 0) {
            state->write_buffer.erase(state->write_buffer.begin(), state->write_buffer.begin() + bytes_sent);
            // If we sent everything, no need to add EVFILT_WRITE yet.
            needs_write_event = !state->write_buffer.empty();
        } else if (bytes_sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // Expected if socket buffer is full, need EVFILT_WRITE
            needs_write_event = true;
        } else if (bytes_sent < 0) {
            // Actual send error during immediate attempt
            log_error(fmt::format("Immediate send failed for conn {}", client_idx));
            close_client_connection(state, kq, "Immediate send failed");
            return; // Don't proceed to register kqueue event
        } else if (bytes_sent == 0) {
             // Should not happen for TCP, treat as error
             log_error(fmt::format("Immediate send returned 0 for conn {}", client_idx));
             close_client_connection(state, kq, "Immediate send returned 0");
             return;
        }
    }


    // Register for write event *only if* data remains in the buffer after the immediate send attempt
    if (needs_write_event && !state->write_buffer.empty()) {
        struct kevent change;
        EV_SET(&change, state->socket_fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, state);
        if (kevent(kq, &change, 1, nullptr, 0, nullptr) == -1) {
            // Log error, but connection might still be usable for reads.
            // handle_write will attempt to send again if called.
            log_error(fmt::format("kevent failed to register EVFILT_WRITE for sending on conn {}", client_idx));
        }
    }
}


// --- Main Function ---

// Loads sentences from the specified file into the global vector.
bool load_sentences(std::string_view filename) {
    console->info("Loading sentences from {}...", filename);
    std::ifstream sentence_file(filename.data()); // Use .data() for C-style API
    std::string line;
    if (sentence_file.is_open()) {
        while (getline(sentence_file, line)) {
            // Remove potential carriage return characters
            line.erase(std::remove(line.begin(), line.end(), '\r'), line.end());
            if (!line.empty()) { // Avoid adding empty lines
                sentences.push_back(line);
            }
        }
        sentence_file.close();
        if (sentences.empty()) {
             console->warn("Warning: {} was empty or could not be read properly.", filename);
             // Decide if this is fatal - for now, allow continuing but log warning.
             return true; // Or false if sentences are mandatory
        } else {
             console->info("Loaded {} sentences.", sentences.size());
             return true;
        }
    } else {
        log_error(fmt::format("Failed to open {}", filename));
        return false;
    }
}

// Initializes the spdlog console logger.
bool initialize_logger() {
    try {
        console = spdlog::stdout_color_mt("console");
        // Example pattern: [Timestamp] [Level] [Thread ID] Message
        console->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
        spdlog::set_default_logger(console);
        console->info("spdlog initialized.");
        return true;
    } catch (const spdlog::spdlog_ex& ex) {
        std::cerr << "Log initialization failed: " << ex.what() << std::endl;
        return false;
    }
}

// Attempts initial connections for all clients.
void attempt_initial_connections(int kq, const struct sockaddr_in& serv_addr) {
    console->info("[Main] Attempting initial connections for {} clients to {}:{}...", NUM_CONNECTIONS, SERVER_IP, PORT);
    int initial_success_or_pending = 0;
    for (int i = 0; i < NUM_CONNECTIONS; ++i) {
        if (attempt_connection(&clients[i], kq, serv_addr)) {
            initial_success_or_pending++;
        }
        // If attempt_connection returns false, it already scheduled a retry.
    }
    console->info("[Main] Initiated/Pending {} initial connection attempts.", initial_success_or_pending);
    log_connection_status_if_changed(); // Log initial status
}

// Checks for and attempts any pending connection retries.
void check_and_attempt_retries(int kq, const struct sockaddr_in& serv_addr) {
    auto now = std::chrono::steady_clock::now();
    for (int client_idx = 0; client_idx < NUM_CONNECTIONS; ++client_idx) {
        ClientState& client = clients[client_idx];
        // Check if disconnected, fd is invalid, retry is scheduled (attempts > 0), and it's time
        if (client.status == ClientStatus::DISCONNECTED &&
            client.socket_fd == -1 &&
            client.retry_attempts > 0 && // retry_attempts == -1 means stopped
            now >= client.next_retry_time)
        {
            console->info("[Conn {}] Attempting scheduled retry (attempt {})...", client_idx, client.retry_attempts);
            attempt_connection(&client, kq, serv_addr);
            // If attempt fails, it will schedule the *next* retry internally.
        }
    }
}

// Processes events received from kqueue.
void process_kevents(struct kevent* events, int nev, int kq) {
    for (int i = 0; i < nev; ++i) {
        struct kevent& current_event = events[i];
        uintptr_t ident = current_event.ident; // File descriptor
        ClientState* state = static_cast<ClientState*>(current_event.udata); // User data points to ClientState

        // --- Event Validation ---
        // 1. Check for kqueue errors on the event itself
        if (current_event.flags & EV_ERROR) {
            std::string error_msg = strerror(current_event.data);
            if (state && state->socket_fd == (int)ident) {
                // Error associated with a known client state
                close_client_connection(state, kq, "kevent error: " + error_msg);
            } else {
                 // Error on an fd not matching our known state (or state is null)
                 console->error("[Event] Kevent error on unknown/mismatched ident {}: {}", ident, error_msg);
                 // Attempt to remove events just in case, although fd might be invalid
                 remove_kqueue_events(kq, (int)ident);
            }
            continue; // Process next event
        }

        // 2. Check if the state pointer is valid and matches the event fd
        //    (Crucial to prevent acting on stale events after a connection is closed and fd potentially reused)
        if (!state || state->socket_fd != (int)ident) {
             // This event is for a socket we no longer manage (or udata was null). Ignore it.
             // console->debug("[Event] Ignoring event for closed/mismatched fd: {}", ident); // Optional debug log
             continue;
        }

        // 3. Check for EOF (server closed connection) - handle before read/write checks
         if (current_event.flags & EV_EOF) {
             // data field might contain error code if EOF is due to an error
             std::string reason = "Server disconnected (EOF)";
             if (current_event.fflags != 0) { // fflags might contain socket error info
                 reason += fmt::format(" (fflags: {}, error: {})", current_event.fflags, strerror(current_event.fflags));
             } else if (current_event.data != 0) { // data might contain error code
                 reason += fmt::format(" (data: {}, error: {})", current_event.data, strerror(current_event.data));
             }
             close_client_connection(state, kq, reason);
             continue; // Process next event
         }

        // --- Event Handling ---
        // At this point, 'state' is valid and matches 'ident' (state->socket_fd)

        // Handle Write Events (Connection completion or ready to send)
        if (current_event.filter == EVFILT_WRITE) {
             handle_write(state, kq);
        }
        // Handle Read Events (Incoming data)
        else if (current_event.filter == EVFILT_READ) {
             // Check available bytes if needed (current_event.data)
             // size_t bytes_available = current_event.data;
             handle_read(state, kq);
        }
    } // End for loop processing events
}

// Calculates final latency statistics and writes them to the result file.
void calculate_and_write_results() {
    console->info("Calculating statistics...");
    std::vector<double> all_latencies;
    int connections_with_data = 0;
    bool test_completed_fully = (total_responses_received.load() >= MAX_REQUESTS_BEFORE_SHUTDOWN);
    std::chrono::duration<double> total_duration = std::chrono::duration<double>::zero();

    if (test_completed_fully && test_timer_started.load()) {
        total_duration = test_end_time - test_start_time;
    }
    for (const auto& client : clients) {
        // Check if the client ever connected and has latency data
        if (!client.latencies_ms.empty()) {
            connections_with_data++;
            all_latencies.insert(all_latencies.end(), client.latencies_ms.begin(), client.latencies_ms.end());
        }
    }

    if (all_latencies.empty()) {
        console->warn("No latency data collected from any connection.");
        std::ofstream result_file(RESULT_FILE);
         if (result_file.is_open()) {
             result_file << "--- Test Results ---\n";
             if (!test_completed_fully) {
                 result_file << "Test interrupted before completion.\n";
             }
             result_file << "Target Requests: " << MAX_REQUESTS_BEFORE_SHUTDOWN << "\n";
             result_file << "Total Responses Received: " << total_responses_received.load() << "\n";
             result_file << "No latency data collected.\n";
             result_file << "Connections with Data: " << connections_with_data << "/" << NUM_CONNECTIONS << "\n";
             result_file.close();
         } else {
             log_error(fmt::format("Failed to open {} to write no-data result", RESULT_FILE));
         }
        return;
    }

    // Sort latencies for percentile calculation
    std::sort(all_latencies.begin(), all_latencies.end());

    double sum = std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0);
    double avg = sum / all_latencies.size();
    double min_lat = all_latencies.front();
    double max_lat = all_latencies.back();

    // Calculate P99 index safely
    size_t p99_index = static_cast<size_t>(all_latencies.size() * 0.99);
    // Ensure index is within bounds (0 to size-1)
    if (p99_index >= all_latencies.size()) {
         p99_index = all_latencies.empty() ? 0 : all_latencies.size() - 1;
    }
    double p99_lat = all_latencies[p99_index]; // Access is safe due to bounds check


    // Log results to console
    console->info("--- Test Results ---");
    if (test_completed_fully) {
         console->info("Test Completed: {} requests in {:.3f} seconds", total_responses_received.load(), total_duration.count());
    } else {
         console->warn("Test Interrupted: Received {} responses before shutdown.", total_responses_received.load());
    }
    console->info("Target Requests: {}", MAX_REQUESTS_BEFORE_SHUTDOWN);
    console->info("Total Responses Measured: {}", all_latencies.size()); // Should match total_responses_received if test completed
    console->info("Connections with Data: {}/{}", connections_with_data, NUM_CONNECTIONS);
    console->info("Average Latency: {:.3f} ms", avg);
    console->info("Minimum Latency: {:.3f} ms", min_lat);
    console->info("Maximum Latency: {:.3f} ms", max_lat);
    console->info("P99 Latency:     {:.3f} ms", p99_lat);

    // Write results to file
    console->info("Writing results to {}...", RESULT_FILE);
    std::ofstream result_file(RESULT_FILE);
    if (result_file.is_open()) {
        result_file << std::fixed << std::setprecision(3); // Set precision for file output
        result_file << "--- Test Results ---\n";
        if (test_completed_fully) {
            result_file << "Test Completed: Yes\n";
            result_file << "Total Duration: " << total_duration.count() << " seconds\n";
        } else {
            result_file << "Test Completed: No (Interrupted)\n";
        }
        result_file << "Target Requests: " << MAX_REQUESTS_BEFORE_SHUTDOWN << "\n";
        result_file << "Total Responses Received: " << total_responses_received.load() << "\n";
        result_file << "Total Latencies Measured: " << all_latencies.size() << "\n";
        result_file << "Connections with Data: " << connections_with_data << "/" << NUM_CONNECTIONS << "\n";
        result_file << "Average Latency: " << avg << " ms\n";
        result_file << "Minimum Latency: " << min_lat << " ms\n";
        result_file << "Maximum Latency: " << max_lat << " ms\n";
        result_file << "P99 Latency:     " << p99_lat << " ms\n";
        result_file.close();
        console->info("Results written successfully.");
    } else {
        log_error(fmt::format("Failed to open {} for writing", RESULT_FILE));
    }
}


int main() {
    // --- Initialization ---
    if (!initialize_logger()) {
        return 1;
    }
    srand(time(nullptr)); // Seed random number generator

    if (!load_sentences(SENTENCE_FILE)) {
        // Error already logged by load_sentences
        spdlog::shutdown();
        return 1;
    }
    // --- End Initialization ---


    console->info("Starting TCP Latency Test Client ({} connections)...", NUM_CONNECTIONS);

    // --- Setup kqueue and Network ---
    int kq = kqueue();
    if (kq == -1) {
        log_error("kqueue() failed");
        spdlog::shutdown();
        return 1;
    }
    console->info("[Main] kqueue created (fd: {})", kq);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    // Use SERVER_IP directly as it's const char*
    if (inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr) <= 0) {
        log_error("Invalid server address or family not supported");
        close(kq);
        spdlog::shutdown();
        return 1;
    }
    // --- End Setup ---

    // --- Start Background Threads ---
    std::thread input_thr(input_thread_func);
    console->info("[Main] Input thread started.");
    // --- End Background Threads ---

    // --- Initial Connection Attempts ---
    attempt_initial_connections(kq, serv_addr);
    // --- End Initial Connections ---


    // --- Main Event Loop ---
    struct kevent events[KEVENT_MAX_EVENTS];
    console->info("[Main] Starting event loop...");
    auto last_retry_check_time = std::chrono::steady_clock::now();
    auto last_status_log_time = std::chrono::steady_clock::now();

    while (running.load()) {
        // Calculate timeout for kevent
        // Use a fixed timeout for simplicity, could be dynamic
        struct timespec timeout_ts = {};
        auto timeout_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(KEVENT_TIMEOUT);
        timeout_ts.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(timeout_ns).count();
        timeout_ts.tv_nsec = (timeout_ns % std::chrono::seconds(1)).count();

        int nev = kevent(kq, nullptr, 0, events, KEVENT_MAX_EVENTS, &timeout_ts);

        if (nev == -1) {
            if (errno == EINTR) continue; // Interrupted by signal, simply restart wait
            log_error("kevent() wait failed");
            running = false; // Signal shutdown on other errors
            break;
        }

        auto now = std::chrono::steady_clock::now();

        // --- Periodic Tasks ---
        // Check for pending retries
        if (now - last_retry_check_time >= RETRY_CHECK_INTERVAL) {
             last_retry_check_time = now;
             check_and_attempt_retries(kq, serv_addr);
        }
        // Log connection status periodically
        if (now - last_status_log_time >= STATUS_LOG_INTERVAL) {
            log_connection_status_if_changed();
            last_status_log_time = now;
        }
        // --- End Periodic Tasks ---


        // --- Process Events ---
        if (nev > 0) {
            process_kevents(events, nev, kq);
        }
        // --- End Process Events ---

    } // End while(running.load())
    // --- End Main Event Loop ---


    // --- Clean up ---
    console->info("[Main] Event loop finished. Cleaning up...");

    // Signal input thread to stop (if it hasn't already) and wait for it
    if (running.load()) { // Check if it wasn't already stopped by input thread
        running = false;
        // Optionally, could try to interrupt std::getline if needed, but signaling should suffice
        // On POSIX, could potentially send a signal or close stdin, but complex.
    }
    if (input_thr.joinable()) {
        input_thr.join();
    }
    console->info("[Main] Input thread joined.");

    // Close remaining connections
    console->info("[Main] Closing remaining client connections...");
    int closed_count = 0;
    for (int i = 0; i < NUM_CONNECTIONS; ++i) {
        if (clients[i].socket_fd != -1) {
            clients[i].retry_attempts = -1; // Prevent retries during shutdown
            // Pass -1 for kq as it's about to be closed anyway, avoids unnecessary kevent calls
            close_client_connection(&clients[i], -1, "Normal shutdown");
            closed_count++;
        }
    }
    console->info("[Main] Closed {} client sockets during shutdown.", closed_count);

    // Close kqueue
    close(kq);
    console->info("[Main] kqueue closed.");
    // --- End Clean up ---


    // --- Final Results ---
    calculate_and_write_results();
    // --- End Final Results ---


    console->info("[Main] Client finished.");
    spdlog::shutdown(); // Shutdown spdlog
    return 0;
}

// --- Implementations for handle_write and handle_read ---

void handle_write(ClientState* state, int kq) {
    // State validity already checked in process_kevents
    int client_idx = state - &clients[0];

    // Handle connection completion for non-blocking connect()
    if (state->status == ClientStatus::CONNECTING) {
        int error = 0;
        socklen_t len = sizeof(error);
        // Check socket error option to confirm connection success
        if (getsockopt(state->socket_fd, SOL_SOCKET, SO_ERROR, &error, &len) == -1 || error != 0) {
            // Connection failed
            int connect_errno = (error != 0) ? error : errno; // Use SO_ERROR if available, else errno from getsockopt
            close_client_connection(state, kq, fmt::format("Async connection failed: {}", strerror(connect_errno)));
            return;
        }
        // Connection successful
        state->status = ClientStatus::CONNECTED;
        state->retry_attempts = 0; // Reset retry count on successful connection
        connected_clients_count++;
        log_connection_status_if_changed();
        if(console) console->info("[Conn {}] Connected successfully (fd: {})", client_idx, state->socket_fd);
        else std::cout << "\n[Conn " << client_idx << "] Connected successfully (fd: " << state->socket_fd << ")" << std::endl;

        // Check if this connection makes all clients connected
        if (connected_clients_count.load() == NUM_CONNECTIONS && !all_clients_connected.load()) {
             all_clients_connected = true;
             console->info(">>> All {} clients connected! Starting test timer and triggering initial sends (max {} requests). <<<", NUM_CONNECTIONS, MAX_REQUESTS_BEFORE_SHUTDOWN);
             // Start the test timer only once
             if (!test_timer_started.load()) {
                 test_start_time = std::chrono::steady_clock::now();
                 test_timer_started = true;
             }
             // Trigger initial send for all *currently* connected clients
             for (int i = 0; i < NUM_CONNECTIONS; ++i) {
                 if (clients[i].status == ClientStatus::CONNECTED) {
                     send_random_sentence(&clients[i], kq); // send_random_sentence now checks the limit
                 }
             }
        }
        // If all clients were already connected (e.g., a reconnect), just try sending for this one client.
        else if (all_clients_connected.load()) {
             send_random_sentence(state, kq);
        }
        // Fall through to potentially write data added by send_random_sentence
    }

    // Proceed to write data if connected and buffer is not empty
    if (state->status == ClientStatus::CONNECTED && !state->write_buffer.empty()) {
        ssize_t bytes_sent = send(state->socket_fd, state->write_buffer.data(), state->write_buffer.size(), 0); // No flags needed for standard send

        if (bytes_sent > 0) {
            // Successfully sent some data, remove it from buffer
            state->write_buffer.erase(state->write_buffer.begin(), state->write_buffer.begin() + bytes_sent);

            // If buffer is now empty, disable the write filter (important!)
            if (state->write_buffer.empty()) {
                struct kevent change;
                // EV_DISABLE might be slightly more efficient than EV_DELETE if we expect to write again soon,
                // but EV_DELETE is clearer conceptually - remove interest when buffer is empty.
                EV_SET(&change, state->socket_fd, EVFILT_WRITE, EV_DELETE, 0, 0, state);
                if (kevent(kq, &change, 1, nullptr, 0, nullptr) == -1) {
                    if (errno != ENOENT && errno != EBADF) { // Ignore if already deleted or bad fd
                         log_error(fmt::format("kevent failed to disable write filter for {}", client_idx));
                    }
                }
            }
            // If buffer still has data, leave EVFILT_WRITE enabled - kqueue will trigger again when ready.
        } else if (bytes_sent == 0) {
            // Should not happen with TCP sockets, indicates an issue.
            close_client_connection(state, kq, "send returned 0");
        } else { // bytes_sent < 0
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Kernel buffer is full. This is expected.
                // EVFILT_WRITE should still be registered, so kqueue will notify us when space is available.
                // No action needed here.
            } else {
                // Actual send error
                log_error(fmt::format("send error from buffer for conn {}", client_idx));
                close_client_connection(state, kq, "send error from buffer");
            }
        }
    } else if (state->status == ClientStatus::CONNECTED && state->write_buffer.empty()) {
         // If connected and buffer is empty, ensure write filter is disabled just in case.
         // This might happen if send_random_sentence added data, tried immediate send,
         // sent everything, and then handle_write was called later without intervening reads/writes.
         struct kevent change;
         EV_SET(&change, state->socket_fd, EVFILT_WRITE, EV_DELETE, 0, 0, state);
         if (kevent(kq, &change, 1, nullptr, 0, nullptr) == -1) {
             if (errno != ENOENT && errno != EBADF) { // Ignore if already deleted or bad fd
                 log_error(fmt::format("kevent failed to disable write filter (buffer empty) for {}", client_idx));
             }
         }
    }
}

void handle_read(ClientState* state, int kq) {
    // State validity already checked in process_kevents
    int client_idx = state - &clients[0];
    char read_buf[BUFFER_SIZE]; // Temporary buffer for read() call

    ssize_t bytes_read = read(state->socket_fd, read_buf, BUFFER_SIZE);

    if (bytes_read > 0) {
        // Append data to the client's persistent read buffer
        state->read_buffer.insert(state->read_buffer.end(), read_buf, read_buf + bytes_read);

        // Process all complete messages (newline terminated) in the buffer
        auto& buf = state->read_buffer; // Alias for brevity
        auto start_iter = buf.begin();
        while (true) {
            // Find the next newline character from the current start position
            auto newline_iter = std::find(start_iter, buf.end(), '\n');

            // If no newline found, break the loop (wait for more data)
            if (newline_iter == buf.end()) {
                break;
            }

            // --- Found a complete message ---
            bool processed_response = false; // Flag to track if we actually processed a valid response

            // Calculate latency if start time is valid
            double latency_ms = -1.0;
            if (state->current_request_start_time != std::chrono::steady_clock::time_point::min()) {
                 auto response_time = std::chrono::steady_clock::now();
                 std::chrono::duration<double, std::milli> latency = response_time - state->current_request_start_time;
                 latency_ms = latency.count();
                 state->latencies_ms.push_back(latency_ms);
                 // Reset start time *only after* successfully processing the response
                 state->current_request_start_time = std::chrono::steady_clock::time_point::min();
                 processed_response = true; // Mark as processed
            } else {
                 // Received response without a corresponding start time. This might happen
                 // if the server sends unsolicited messages or if our timing logic has a flaw.
                 if(console) console->warn("[Conn {}] Received response without a recorded start time. Ignoring for stats.", client_idx);
            }

            // --- Message Processed ---

            // Remove processed message (including newline) from the beginning of the buffer
            buf.erase(buf.begin(), newline_iter + 1);

            // Reset start_iter for the next search
            start_iter = buf.begin();

            // --- Check Test Completion & Trigger Next Send ---
            if (processed_response) {
                int completed_count = total_responses_received.fetch_add(1) + 1; // Increment and get new value

                if (completed_count == MAX_REQUESTS_BEFORE_SHUTDOWN) {
                    // This is the final response!
                    test_end_time = std::chrono::steady_clock::now(); // Record end time
                    console->info(">>> Target of {} responses reached! Signaling shutdown. <<<", MAX_REQUESTS_BEFORE_SHUTDOWN);
                    running = false; // Signal main loop to exit
                    // Don't send another sentence
                } else if (completed_count < MAX_REQUESTS_BEFORE_SHUTDOWN) {
                    // Trigger next send if we haven't reached the limit
                    send_random_sentence(state, kq);
                }
                // If completed_count > MAX_REQUESTS_BEFORE_SHUTDOWN (due to race condition), do nothing extra.
            } else {
                 // If we didn't process a valid response (e.g., no start time),
                 // should we still send another sentence? Depends on protocol.
                 // Assuming we only send after a valid response for this test:
                 // No call to send_random_sentence here.
            }

        } // End while loop processing messages in buffer

        // If buffer has remaining partial data, start_iter will be buf.begin()
        // and the next read will append to it.

    } else if (bytes_read == 0) {
        // Server closed connection gracefully (EOF)
        // This should ideally be caught by the EV_EOF flag in process_kevents.
        // If reached here, it means EV_EOF might not have been set yet, but read returned 0.
        close_client_connection(state, kq, "Read returned 0 (EOF)");
    } else { // bytes_read < 0
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // Normal for non-blocking socket, no data available right now.
            // kqueue will notify us again when data arrives. No action needed.
        } else {
            // Actual read error
            log_error(fmt::format("read error for conn {}", client_idx));
            close_client_connection(state, kq, "read error");
        }
    }
}