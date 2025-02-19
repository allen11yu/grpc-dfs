#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "../service/dfs-service.grpc.pb.h"
// #include "src/dfslibx-call-data.h"
// #include "src/dfslibx-service-runner.h"
#include "../shared/dfslib-shared.h"
#include "dfslib-servernode.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;

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

using FileRequestType = ListRequest;
using FileListResponseType = ListResponse;

extern dfs_log_level_e DFS_LOG_LEVEL;

class DFSServiceImpl final : public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
                             public DFSCallDataManager<FileRequestType, FileListResponseType>
{

private:
    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** Mutex for write locks map **/
    std::mutex write_locks_mutex;

    /** Map for server to track which client holds lock **/
    std::unordered_map<std::string, std::string> write_locks;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath)
    {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

public:
    DFSServiceImpl(const std::string &mount_path, const std::string &server_address, int num_async_threads) : mount_path(mount_path), crc_table(CRC::CRC_32())
    {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]
                                               { this->ProcessQueuedRequests(); });
    }

    ~DFSServiceImpl()
    {
        this->runner.Shutdown();
    }

    void Run()
    {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext *context,
                         FileRequestType *request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType> *response,
                         grpc::ServerCompletionQueue *cq,
                         void *tag)
    {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);
    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext *context, FileRequestType *request, FileListResponseType *response)
    {
        std::cout << "Begin ProcessCallback" << std::endl;
        DIR *dir = opendir(mount_path.c_str());
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL)
        {
            if (entry->d_type == DT_REG)
            {
                std::cout << "Regular file detected: " << entry->d_name << std::endl;
                auto *fileinfo = response->add_fileinfo();
                fileinfo->set_filename(entry->d_name);

                struct stat filestat;
                if (stat(WrapPath(entry->d_name).c_str(), &filestat) == 0)
                {
                    fileinfo->set_mtime(filestat.st_mtime);
                }
            }
        }
        closedir(dir);
        std::cout << "End ProcessCallback" << std::endl;
    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests()
    {
        while (true)
        {
            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);

                for (QueueRequest<FileRequestType, FileListResponseType> &queue_request : this->queued_tags)
                {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                                              queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                                            this->queued_tags.begin(),
                                            this->queued_tags.end(),
                                            [](QueueRequest<FileRequestType, FileListResponseType> &queue_request)
                                            { return queue_request.finished; }),
                                        this->queued_tags.end());
            }
        }
    }

    /**
     * @brief Lists all files available on the server.
     */
    Status DFSList(ServerContext *context,
                   const ListRequest *request,
                   ListResponse *response) override
    {
        if (context->IsCancelled())
        {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
        }

        DIR *dir = opendir(mount_path.c_str());
        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL)
        {
            if (entry->d_type == DT_REG)
            {
                auto *fileinfo = response->add_fileinfo();
                fileinfo->set_filename(entry->d_name);

                struct stat filestat;
                if (stat(WrapPath(entry->d_name).c_str(), &filestat) == 0)
                {
                    fileinfo->set_mtime(filestat.st_mtime);
                }
                else
                {
                    return Status(StatusCode::CANCELLED, "Error listing files");
                }
            }
        }
        closedir(dir);
        return Status::OK;
    }

    /**
     * @brief Get file status from the server.
     */
    Status DFSStatus(ServerContext *context,
                     const StatusRequest *request,
                     StatusResponse *response) override
    {
        std::string filename = request->filename();
        if (context->IsCancelled())
        {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
        }

        struct stat filestat;
        if (stat(WrapPath(filename).c_str(), &filestat) == 0)
        {
            // file found
            response->set_filename(filename);
            response->set_size(filestat.st_size);
            response->set_mtime(filestat.st_mtime);
            response->set_ctime(filestat.st_ctime);

            std::uint32_t server_crc = dfs_file_checksum(WrapPath(filename), &this->crc_table);
            response->set_crc(server_crc);

            return Status::OK;
        }
        else
        {
            // file not found
            return Status(StatusCode::NOT_FOUND, "The requested file is not found");
        }
    }

    /**
     * @brief Fetch file content from the server.
     */
    Status DFSGetFile(ServerContext *context,
                      const dfs_service::GetRequest *request,
                      ServerWriter<GetResponse> *writer) override
    {
        std::string filename = request->filename();
        std::ifstream filestream(WrapPath(filename), std::ios::binary);

        // Check if file exist. If not, return status message
        if (!filestream.is_open())
        {
            return Status(StatusCode::NOT_FOUND, "The requested file is not found");
        }

        // Send file data in chunks
        char buffer[256];
        while (!filestream.eof())
        {
            // Continously check if deadline exceed
            if (context->IsCancelled())
            {
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
            }
            filestream.read(buffer, sizeof(buffer));
            GetResponse response;
            response.set_filechunk(buffer, filestream.gcount());
            writer->Write(response);
        }
        return Status::OK;
    }

    /**
     * @brief Request lock from the server.
     */
    Status DFSRequestLock(ServerContext *context,
                          const LockRequest *request,
                          LockResponse *response) override
    {
        std::string filename = request->filename();
        std::string cid = request->cid();
        if (context->IsCancelled())
        {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
        }

        std::lock_guard<std::mutex> lock(write_locks_mutex);
        // critical section
        if (write_locks[filename] == "" || write_locks[filename] == cid)
        {
            // grant access
            write_locks[filename] = cid;
            response->set_locked(true);
            return Status::OK;
        }
        else
        {
            // don't grant access
            response->set_locked(false);
            return Status(StatusCode::RESOURCE_EXHAUSTED, "write lock cannot be obtained");
        }

        // mutex unlock
    }

    /**
     * @brief Store file content to the server.
     */
    Status DFSStoreFile(ServerContext *context,
                        ServerReader<StoreRequest> *reader,
                        StoreResponse *response) override
    {
        StoreRequest request;
        bool new_file = true;

        std::string filename;
        std::ofstream stored_file;
        while (reader->Read(&request))
        {
            // Continously check if deadline exceed
            if (context->IsCancelled())
            {
                std::lock_guard<std::mutex> lock(write_locks_mutex);
                // releasing the lock
                write_locks.erase(filename);
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
            }

            // replace existing file content
            if (new_file)
            {
                filename = request.filename();
                stored_file.open(WrapPath(filename), std::ios::binary);
                new_file = false;
            }
            std::cout << "Server: storing the file: " << filename << std::endl;

            stored_file.write(request.filechunk().data(), request.filechunk().size());
        }

        std::lock_guard<std::mutex> lock(write_locks_mutex);
        // releasing the lock
        write_locks.erase(filename);

        return Status::OK;
    }

    /**
     * @brief Delete file from the server.
     */
    Status DFSDeleteFile(ServerContext *context,
                         const DeleteRequest *request,
                         DeleteResponse *response) override
    {
        std::string filename = request->filename();

        std::lock_guard<std::mutex> lock(write_locks_mutex);
        if (context->IsCancelled())
        {
            // releasing the lock
            write_locks.erase(filename);
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded");
        }

        if (std::remove(WrapPath(filename).c_str()) == 0)
        {
            // file deleted
            // releasing the lock
            write_locks.erase(filename);
            return Status::OK;
        }
        else
        {
            // releasing the lock
            write_locks.erase(filename);
            return Status(StatusCode::CANCELLED, "Something happened");
        }
    }
};

/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
                             const std::string &mount_path,
                             int num_async_threads,
                             std::function<void()> callback) : server_address(server_address),
                                                               mount_path(mount_path),
                                                               num_async_threads(num_async_threads),
                                                               grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept
{
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start()
{
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);

    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}
