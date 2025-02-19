#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

//#include "src/dfs-utils.h"
//#include "src/dfslibx-clientnode-p2.h"
#include "../shared/dfslib-shared.h"
#include "dfslib-clientnode.h"
#include "../service/dfs-service.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using grpc::StatusCode;

using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::chrono::time_point;

using dfs_service::DeleteRequest;
using dfs_service::DeleteResponse;
using dfs_service::DFSService;
using dfs_service::GetRequest;
using dfs_service::GetResponse;
using dfs_service::ListRequest;
using dfs_service::ListResponse;
using dfs_service::LockRequest;
using dfs_service::LockResponse;
using dfs_service::StatusRequest;
using dfs_service::StatusResponse;
using dfs_service::StoreRequest;
using dfs_service::StoreResponse;

extern dfs_log_level_e DFS_LOG_LEVEL;

using FileRequestType = ListRequest;
using FileListResponseType = ListResponse;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

/**
 * @brief Requests a write lock for a given file at the server, ensuring that the
 *        current client becomes the sole creator/writer for that file.
 *
 * This method communicates with the server to obtain a write lock for the specified
 * file. If the server responds with a RESOURCE_EXHAUSTED error, indicating that the
 * lock cannot be obtained, the client cancels the current file storage operation.
 *
 * @param filename The name of the file for which the write lock is being requested.
 * @return StatusCode The status of the request:
 * - StatusCode::OK if the lock is successfully acquired.
 * - StatusCode::DEADLINE_EXCEEDED if the timeout deadline is reached before a response is received.
 * - StatusCode::RESOURCE_EXHAUSTED if the server cannot provide the write lock.
 * - StatusCode::CANCELLED if the operation is canceled due to an error or timeout.
 */
grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename)
{
    LockRequest request;
    request.set_filename(filename);
    request.set_cid(client_id);

    // Set deadline
    ClientContext context;
    auto deadline = std::chrono::system_clock::now() +
                    std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);

    LockResponse response;
    Status status = service_stub->DFSRequestLock(&context, request, &response);

    if (status.ok())
    {
        // lock obtained
        return StatusCode::OK;
    }
    else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        return StatusCode::DEADLINE_EXCEEDED;
    }
    else if (status.error_code() == StatusCode::RESOURCE_EXHAUSTED)
    {
        return StatusCode::RESOURCE_EXHAUSTED;
    }
    else
    {
        return StatusCode::CANCELLED;
    }
}

/**
 * @brief Connects to the gRPC service to store a file while ensuring that the file
 *        is not already present on the server. A write lock is requested before
 *        attempting to store the file, and the operation is canceled if the lock
 *        cannot be obtained.
 *
 * This method first checks if the file already exists on the server. If the server
 * responds with an ALREADY_EXISTS error, indicating the file has not changed,
 * the operation is skipped. Otherwise, a write lock is requested before proceeding
 * to store the file. If the lock request fails (due to RESOURCE_EXHAUSTED error),
 * the operation is canceled, and a RESOURCE_EXHAUSTED status is returned.
 *
 * @param filename The name of the file to be stored on the server.
 * @return StatusCode The status of the operation:
 * - StatusCode::OK if the file is successfully stored.
 * - StatusCode::DEADLINE_EXCEEDED if the timeout deadline is reached before a response is received.
 * - StatusCode::ALREADY_EXISTS if the file on the server is identical to the local cached file.
 * - StatusCode::RESOURCE_EXHAUSTED if the write lock cannot be obtained.
 * - StatusCode::CANCELLED if the operation is canceled due to an error or timeout.
 */
grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename)
{
    FileStatus file_status;
    StatusCode status = Stat(filename, &file_status);
    if (status == StatusCode::OK || status == StatusCode::NOT_FOUND)
    {
        // compare client and server mtime via crc
        std::uint32_t client_crc = dfs_file_checksum(WrapPath(filename), &this->crc_table);
        if (client_crc != static_cast<uint32_t>(file_status.server_crc))
        { // diff in client and server crc
            // request lock
            StatusCode lock_status = RequestWriteAccess(filename);
            if (lock_status == StatusCode::OK)
            {
                // perform store
                // Open file and check for existence
                std::ifstream filestream(WrapPath(filename), std::ios::binary);
                if (!filestream.is_open())
                {
                    return StatusCode::NOT_FOUND;
                }

                StoreRequest request;
                request.set_filename(filename);

                // Set deadline
                ClientContext context;
                auto deadline = std::chrono::system_clock::now() +
                                std::chrono::milliseconds(deadline_timeout);
                context.set_deadline(deadline);

                StoreResponse response;
                std::unique_ptr<ClientWriter<StoreRequest>> writer(service_stub->DFSStoreFile(&context, &response));

                // Send file data in chunks
                char buffer[256];
                while (!filestream.eof())
                {
                    filestream.read(buffer, sizeof(buffer));
                    request.set_filechunk(buffer, filestream.gcount());
                    writer->Write(request);
                }
                writer->WritesDone();
                Status writer_status = writer->Finish();

                // Check status and return corresponding status
                if (writer_status.ok())
                {
                    return StatusCode::OK;
                }
                else if (writer_status.error_code() == StatusCode::DEADLINE_EXCEEDED)
                {
                    return StatusCode::DEADLINE_EXCEEDED;
                }
                else
                {
                    return StatusCode::CANCELLED;
                }
            }
            else
            {
                return lock_status;
            }
        }
        else
        { // no diff in files
            // update mtime
            struct utimbuf recent;
            recent.modtime = file_status.mtime;
            utime(WrapPath(filename).c_str(), &recent);
            std::cout << "Client Store: mod time updated to be equal" << std::endl;

            return StatusCode::ALREADY_EXISTS;
        }
    }
    else
    {
        return status;
    }
}

/**
 * @brief Connects to the gRPC service to fetch a file, checking if the file on the
 *        server differs from the local cached version. The file is only fetched if
 *        it has been modified on the server.
 *
 * This method requests a file from the server and compares the modification time (mtime)
 * of the local cached file with the server's mtime. If the files are identical (i.e.,
 * the local file's mtime matches the server's mtime), the fetch operation is skipped.
 * If the file is not found on the server, a NOT_FOUND error is returned. In case
 * of a timeout, DEADLINE_EXCEEDED is returned.
 *
 * @param filename The name of the file to be fetched from the server.
 * @return StatusCode The status of the fetch operation:
 * - StatusCode::OK if the file is successfully fetched.
 * - StatusCode::DEADLINE_EXCEEDED if the timeout deadline is reached before a response is received.
 * - StatusCode::NOT_FOUND if the file cannot be found on the server.
 * - StatusCode::ALREADY_EXISTS if the local cached file has not changed since the last fetch.
 * - StatusCode::CANCELLED if the operation is canceled due to an error or timeout.
 */
grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename)
{
    // Get the file status
    FileStatus file_status;
    StatusCode status = Stat(filename, &file_status);
    if (status == StatusCode::OK)
    {
        // compare client and server mtime via crc
        std::uint32_t client_crc = dfs_file_checksum(WrapPath(filename), &this->crc_table);
        if (client_crc != static_cast<uint32_t>(file_status.server_crc))
        { // diff in client and server crc
            GetRequest request;
            request.set_filename(filename);

            // Set deadline
            ClientContext context;
            auto deadline = std::chrono::system_clock::now() +
                            std::chrono::milliseconds(deadline_timeout);
            context.set_deadline(deadline);

            std::unique_ptr<ClientReader<GetResponse>> reader(service_stub->DFSGetFile(&context, request));

            // Read file chunks and write to the file
            GetResponse response;
            std::ofstream downloaded_file(WrapPath(filename), std::ios::binary);
            while (reader->Read(&response))
            {
                downloaded_file.write(response.filechunk().data(), response.filechunk().size());
            }

            // Check status and return corresponding status
            Status status = reader->Finish();
            if (status.ok())
            {
                return StatusCode::OK;
            }
            else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
            {
                return StatusCode::DEADLINE_EXCEEDED;
            }
            else if (status.error_code() == StatusCode::NOT_FOUND)
            {
                return StatusCode::NOT_FOUND;
            }
            else
            {
                return StatusCode::CANCELLED;
            }
        }
        else
        { // no diff in files
            struct utimbuf recent;
            recent.modtime = file_status.mtime;
            utime(WrapPath(filename).c_str(), &recent);
            std::cout << "Client Fetch: mod time updated to be equal" << std::endl;
            return StatusCode::ALREADY_EXISTS;
        }
    }
    else if (status == StatusCode::DEADLINE_EXCEEDED)
    {
        return StatusCode::DEADLINE_EXCEEDED;
    }
    else if (status == StatusCode::NOT_FOUND)
    {
        return StatusCode::NOT_FOUND;
    }
    else
    {
        return StatusCode::CANCELLED;
    }
}

/**
 * @brief Connects to the gRPC service to delete a file after acquiring a write lock.
 *        If the write lock request fails, the operation is canceled, and a RESOURCE_EXHAUSTED
 *        status is returned.
 *
 * This method requests a write lock for the specified file before attempting to delete it.
 * If the lock is not acquired (due to a RESOURCE_EXHAUSTED error), the operation is canceled.
 * If the delete operation is successful, it returns StatusCode::OK. If there is a timeout,
 * StatusCode::DEADLINE_EXCEEDED is returned.
 *
 * @param filename The name of the file to be deleted.
 * @return StatusCode The status of the delete operation:
 * - StatusCode::OK if the file is successfully deleted.
 * - StatusCode::DEADLINE_EXCEEDED if the timeout deadline is reached before a response is received.
 * - StatusCode::RESOURCE_EXHAUSTED if the write lock cannot be obtained.
 * - StatusCode::CANCELLED if the operation is canceled due to an error or timeout.
 */
grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename)
{
    StatusCode lock_status = RequestWriteAccess(filename);
    if (lock_status == StatusCode::OK)
    {
        // lock granted
        DeleteRequest request;
        request.set_filename(filename);

        // Set deadline
        ClientContext context;
        auto deadline = std::chrono::system_clock::now() +
                        std::chrono::milliseconds(deadline_timeout);
        context.set_deadline(deadline);

        DeleteResponse response;
        Status status = service_stub->DFSDeleteFile(&context, request, &response);

        // Check status and return corresponding status
        if (status.ok())
        {
            return StatusCode::OK;
        }
        else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
        {
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else if (status.error_code() == StatusCode::NOT_FOUND)
        {
            return StatusCode::NOT_FOUND;
        }
        else
        {
            return StatusCode::CANCELLED;
        }
    }
    else
    {
        return lock_status;
    }
}

/**
 * @brief Connects to the gRPC service to list all files and optionally displays the file details.
 *
 * This method sends a request to the server to list all available files. The file names
 * and their corresponding modification times (mtime) are stored in the provided `file_map`.
 * The method also supports an optional `display` parameter that, when set to `true`, will
 * print out the details of the files (e.g., names and modification times).
 *
 * The method returns a `StatusCode` based on the result of the gRPC operation:
 * - StatusCode::OK if the files are successfully listed and processed.
 * - StatusCode::DEADLINE_EXCEEDED if the request times out before a response is received.
 * - StatusCode::CANCELLED if the request is cancelled due to an error or timeout.
 *
 * @param file_map A pointer to a `std::map<std::string, int>` where the file names (key)
 *                 and their corresponding modification times (mtime) (value) will be stored.
 *                 The map is populated with the server's file list upon a successful response.
 * @param display A boolean flag that, when set to `true`, will print the file details
 *                (such as names and modification times) to the console.
 * @return grpc::StatusCode The status of the list operation:
 * - grpc::StatusCode::OK if the files are listed successfully.
 * - grpc::StatusCode::DEADLINE_EXCEEDED if the operation times out.
 * - grpc::StatusCode::CANCELLED if the operation is cancelled or fails due to an error.
 */
grpc::StatusCode DFSClientNodeP2::List(std::map<std::string, int> *file_map, bool display)
{
    ListRequest request;

    // Set deadline
    ClientContext context;
    auto deadline = std::chrono::system_clock::now() +
                    std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);

    ListResponse response;
    Status status = service_stub->DFSList(&context, request, &response);

    if (status.ok())
    {
        for (const auto &info : response.fileinfo())
        {
            file_map->insert({info.filename(), info.mtime()});
            if (display)
            {
                std::cout << "filename: " << info.filename() << ", mtime: " << info.mtime() << std::endl;
            }
        }
        return StatusCode::OK;
    }
    else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        return StatusCode::DEADLINE_EXCEEDED;
    }
    else
    {
        return StatusCode::CANCELLED;
    }
}

/**
 * @brief Connects to the gRPC service to retrieve the status of a specific file.
 *        The status includes the filename, size, modification time (mtime), creation time (ctime),
 *        and server-side CRC (server_crc) values.
 *
 * This method sends a request to the server to get the status of a specified file. The server's
 * response includes the file's metadata (filename, size, mtime, ctime, and server_crc), which is
 * stored in the provided `file_status` pointer.
 *
 * @param filename The name of the file whose status is to be retrieved.
 * @param file_status A pointer to a structure that will hold the file status, including:
 *                    - `filename`: The name of the file.
 *                    - `size`: The size of the file.
 *                    - `mtime`: The modification time of the file.
 *                    - `ctime`: The creation time of the file.
 *                    - `server_crc`: The server-side CRC checksum of the file.
 * @return grpc::StatusCode The status of the operation:
 * - grpc::StatusCode::OK if the file status is retrieved successfully.
 * - grpc::StatusCode::DEADLINE_EXCEEDED if the operation times out.
 * - grpc::StatusCode::NOT_FOUND if the file is not found on the server.
 * - grpc::StatusCode::CANCELLED if the operation is cancelled or fails due to an error.
 */
grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void *file_status)
{
    StatusRequest request;
    request.set_filename(filename);

    // Set deadline
    ClientContext context;
    auto deadline = std::chrono::system_clock::now() +
                    std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(deadline);

    StatusResponse response;
    Status status = service_stub->DFSStatus(&context, request, &response);

    if (status.ok())
    {
        // updating the file_status struct
        FileStatus *status = static_cast<FileStatus *>(file_status);
        status->filename = response.filename();
        status->size = response.size();
        status->mtime = response.mtime();
        status->ctime = response.ctime();
        status->server_crc = response.crc();

        // std::cout << "filename: " << response.filename() << std::endl;
        // std::cout << "size: " << response.size() << std::endl;
        // std::cout << "mtime: " << response.mtime() << std::endl;
        // std::cout << "ctime: " << response.ctime() << std::endl;
        // std::cout << "server_crc: " << response.crc() << std::endl;

        return StatusCode::OK;
    }
    else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
    {
        return StatusCode::DEADLINE_EXCEEDED;
    }
    else if (status.error_code() == StatusCode::NOT_FOUND)
    {
        return StatusCode::NOT_FOUND;
    }
    else
    {
        return StatusCode::CANCELLED;
    }
}

/**
 * @brief Handles file system events triggered by inotify, calling a provided callback.
 *
 * This method is invoked each time inotify signals a change in the file system, such as
 * when a file is modified or created. It executes the provided callback to respond
 * to these changes. Since inotify events occur on a separate thread, careful consideration
 * should be given to concurrency and synchronization between the watcher and other asynchronous
 * operations (e.g., server callbacks).
 *
 * The method may use synchronization mechanisms (such as mutexes or other thread-safe structures)
 * to ensure proper coordination between the inotify watcher and asynchronous callbacks associated
 * with the server. The provided callback should be executed within the thread-safe context to avoid
 * race conditions.
 *
 * @param callback A `std::function<void()>` representing the callback to be executed when
 *                 an inotify event occurs. The callback will be called with the necessary
 *                 thread-safety measures in place to ensure coordination with other async operations.
 */
void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback)
{
    // mutex guard before callback()
    std::lock_guard<std::mutex> lock(watcher_handle_mutex);
    callback();
}

/**
 * @brief Synchronizes the file list between the server and the client and ensures thread safety.
 *
 * This method is invoked when the server responds to an asynchronous request for the `CallbackList`.
 * The goal of this method is to synchronize the files between the server and the client, ensuring that
 * the client’s file system reflects the most recent state from the server.
 *
 * Since the method operates within an asynchronous environment, special care must be taken to synchronize
 * the file list with the server’s state. Additionally, it must coordinate the interactions between the
 * async thread (handling the server response) and the file watcher thread (which responds to file system
 * changes). Both threads could potentially interfere with each other, leading to race conditions if not handled properly.
 *
 * The method should block until the next result is available in the completion queue to ensure that
 * the file synchronization is completed before proceeding further.
 *
 * @return void
 */
void DFSClientNodeP2::HandleCallbackList()
{

    void *tag;

    bool ok = false;

    while (completion_queue.Next(&tag, &ok))
    {
        {
            std::lock_guard<std::mutex> lock(watcher_handle_mutex);

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";
            if (!ok)
            {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok())
            {

                dfs_log(LL_DEBUG3) << "Handling async callback ";
                for (const auto &info : call_data->reply.fileinfo())
                {
                    // compute client file stat
                    struct stat filestat;
                    std::string filename = info.filename();
                    if (stat(WrapPath(filename).c_str(), &filestat) == 0)
                    {
                        // larger mtime is more recent
                        if (filestat.st_mtime > info.mtime())
                        { // client has more recent mtime
                            std::cout << "Storing existing file to server: " << filename << std::endl;
                            Store(filename);
                        }
                        else if (filestat.st_mtime < info.mtime())
                        { // server has more recent mtime
                            std::cout << "Fetching existing file from server: " << filename << std::endl;
                            Fetch(filename);
                        }
                    }
                    else
                    {
                        // Server has a file that client don't
                        std::cout << "Fetching new file from server: " << filename << std::endl;
                        Fetch(filename);
                    }
                }
            }
            else
            {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;
        }

        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();
    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 */
void DFSClientNodeP2::InitCallbackList()
{
    CallbackList<FileRequestType, FileListResponseType>();
}
