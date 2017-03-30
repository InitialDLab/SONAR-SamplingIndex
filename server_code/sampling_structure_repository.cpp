/*
Copyright 2017 InitialDLab

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
// Implementation of the sampling structure repository.  This keeps track of multiple
// sampling structures which can be queried

#include <map>
#include <memory>
#include <list>
#include <time.h>
#include <string>
#include <fstream>
#include <algorithm>

#include <sys/stat.h>

#include <glog/logging.h>

#include "sampling_structure_repository.h"

#include "server_code/protobuf/sampling_api.pb.h"
#include "server_code/protobuf/server_state.pb.h"
#include "sampling_structure.h"
#include "RStree_basic.h"

sampling_structure_repository::sampling_structure_repository(bool restore_checkpoint)
    : mp_repository()
    , m_checkpointFile("SSR_save.bin")
{
    LOG(INFO) << "Starting a sample structure repository";
    mp_repository.reset(new map_t());

    if (restore_checkpoint)
    {
        struct stat buf;
        if (stat(m_checkpointFile.c_str(), &buf) != -1)
        {

            LOG(INFO) << "Attempting to recover from last checkpoint";

            std::fstream input(m_checkpointFile, std::ios::in | std::ios::binary);
            ServerState::SamplingStructureRepoDump state;
            state.ParseFromIstream(&input);

            init_from_protobuf(state);
        }
        else
        {
            LOG(WARNING) << "An attempt was made to load from checkpoint, but the checkpoint file could not be found";
        }
    }
}


sampling_structure_repository::~sampling_structure_repository()
{
    LOG(INFO) << "Shutting down a sample structure repository";
}

grpc::Status sampling_structure_repository::add_new_structure(std::shared_ptr<sampling_structure> new_structure, std::string name)
{
    grpc::Status status;

    LOG(INFO) << "Starting to add a new structure.  Here before locking";

    m_Lock.lock();

    LOG(INFO) << "Recieved lock to add the new structure";

    // if it is tagged as a 'force', we will drop the structure before inserted the new value
    auto it = mp_repository->find(name);
    if (it == mp_repository->end())
        it = mp_repository->insert(mp_repository->begin(), std::pair < std::string, std::shared_ptr<sampling_structure> >(name, nullptr));

    if (it->second != nullptr) // if it points to something, make sure we remove old files when it is deleted.
        it->second->set_cleanupAfterUse(true);

    // check to see if that name already exists in the repository.  If it does we ignore the request
    // otherwise, insert the element

    it->second = new_structure;

    status = grpc::Status(grpc::Status::OK);

    m_Lock.unlock();

    autoSave();

    return status;
}

grpc::Status sampling_structure_repository::add_new_structure(const serverProto::BuildRequest &new_structure_information, serverProto::BuildResponse *response)
{
    grpc::Status status;

    // get the name of the new structure
    std::string name = new_structure_information.name();
    bool force = new_structure_information.force();

    // lets make sure we have characters and numbers in the name (to make things more easy)
    if (!std::all_of(name.begin(), name.end(), [](char i){return isalnum(i) || i == '.' || i == '_'; }))
    {
        LOG(WARNING) << "An attempt was made to build a data structure with name \'" << name << "\' which contains invalid characters";
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Name must only contains alpha numeric characters");
    }

    LOG(INFO) << "Just before recieving lock 1 in add_new_structure";

    // check for preconditions to make sure it is valid to insert the structure.  Take the lock
    // so we know it will not change while we are looking over everything.
    m_Lock.lock();

    auto ptr = mp_repository->find(name);
    if (ptr != mp_repository->end() && !(ptr->second))
    {
        LOG(WARNING) << "An attempt was made to build a data structure which is currently being built: " << name;
        response->set_error_information("A structure with that name is currently being built");
        m_Lock.unlock();
        return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "An item with that name is being built in the database");
    }

    // if it exists and force is not set
    if (!force && mp_repository->count(name) > 0)
    {
        LOG(WARNING) << "An attempt was made to build a data structure which already exists (without force being set): " << name;
        response->set_error_information("A data structure with that name already exists");
        m_Lock.unlock();
        return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "An item with this name already exists in the database");
    }
    // if it does exist and force is set (but it points to a nullptr), we will assume it is currently being built


    // otherwise, if we are going to build it and it does not currently exist we will mark it as building by placing
    // a sentinel with nullptr with the given name
    LOG(INFO) << "submitting and inserting into build";
    mp_repository->insert(std::pair < std::string, std::shared_ptr<sampling_structure> >(name, nullptr));
    LOG(INFO) << "built, now we are unlocking and getting out of here";

    m_Lock.unlock();


    // create the appropriate sampling structure
    std::shared_ptr<sampling_structure> new_structure;
    switch (new_structure_information.payload_type())
    {
    case serverProto::SampleStructureType::NO_PAYLOAD:
        new_structure = RStree_basic::build_RStree_basic(new_structure_information.input_location(), name);
        break;
    case serverProto::SampleStructureType::INT_PAYLOAD:
    case serverProto::SampleStructureType::FLOAT_PAYLOAD:
    default:
        //  UNSUPPORTED PAYLOAD TYPE

        m_Lock.lock();
        mp_repository->erase(name);
        m_Lock.unlock();

        if (response != NULL)
            response->set_error_information("Unsupported sampling type");
        return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "This sampling type has not been implemented yet");
    }

    if (!new_structure)
    {
        m_Lock.lock();
        mp_repository->erase(name);
        m_Lock.unlock();

        LOG(WARNING) << "An attempt was made to build a data structure, but the input file could not be found";
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "The input file could not be found");
    }


    // insert that sampling structure in the repository
    status = add_new_structure(new_structure, name);

    if (!status.ok())
        return status;

    auto elm = mp_repository->find(name);

    if (elm != mp_repository->end())
    {
        response->mutable_structure()->set_name(elm->first);
        response->mutable_structure()->set_count(elm->second->get_size());
        response->mutable_structure()->set_payload_type(elm->second->getPayloadType());

        long secs_since_rebuild = difftime(time(nullptr), elm->second->get_construction_time());
        response->mutable_structure()->set_sec_since_rebuild(secs_since_rebuild);

        LOG(INFO) << "The data structure \'" << name << "\' has been inserted into the repository [size=" << elm->second->get_size() << "]";
    }
    else
    {
        m_Lock.lock();
        auto it = mp_repository->find(name);
        if (it->second == nullptr)
            mp_repository->erase(it);
        m_Lock.unlock();

        LOG(ERROR) << "Unable to insert new data structure \'" << name << "\' (it may already exist)";
        response->set_error_information("Unknown error");
    }

    // TODO: remove input if requested (and successful)

    return status;
}

// list the names of all the structures stored in this repository
void sampling_structure_repository::list_structures_proto(serverProto::ListStructuresResponse& response) const
{
    // I am obtaining a copy, because I don't want the pointer to change while I am walking
    // the tree.  The pointer could change during drop_structure or add_structure
    m_Lock.lock();
    std::shared_ptr<map_t> index = mp_repository;

    response.Clear();

    for (auto t = index->begin(); t != index->end(); ++t)
    {
        auto item = response.add_structures();
        item->set_name(t->first);
        // if it is nullptr we will assume it is currently being built.  We will
        // report that it exists, but the values will be 0
        if (t->second != nullptr)
        {
            item->set_count(t->second->get_size());
            item->set_payload_type(t->second->getPayloadType());

            // calculate the number of seconds since it was rebuilt
            long secs_since_rebuild = difftime(time(nullptr), t->second->get_construction_time());
            item->set_sec_since_rebuild(secs_since_rebuild);
        }
        else
        {
            item->set_count(0);

            // time since rebuild is -1, meaning it has not been built
            item->set_sec_since_rebuild(-1);
        }
    }
    m_Lock.unlock();
}

// get a pointer to a particular structure
std::shared_ptr<sampling_structure> sampling_structure_repository::get_structure(std::string name)
{
    m_Lock.lock();
    std::shared_ptr<map_t> index = mp_repository;

    // if we can not find the structure, return a null pointer
    if (index->count(name) == 0){
        m_Lock.unlock();
        return std::shared_ptr<sampling_structure>(nullptr);
    }

    auto item = index->at(name);
    m_Lock.unlock();
    return item;
}

// drop a particular structure from this repository
// returns true if it was removed.
grpc::Status sampling_structure_repository::drop_structure(std::string name)
{
    grpc::Status status;

    m_Lock.lock();
    // check to see if that name already exists in the repository.  If it does we ignore the request
    // otherwise, insert the element
    if (mp_repository->count(name) != 0)
    {
        // this will be true if it is pointing to a null pointer (meaning it is currently being built)
        if (!mp_repository->find(name)->second)
        {
            LOG(WARNING) << "An attempt was made to drop a structure which is being built";
            status = grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "That request is already being processed");
        }
        else{
            std::shared_ptr<map_t> temp_map(new map_t(*mp_repository));

            // notify the backend to clean up the data file
            temp_map->find(name)->second->set_cleanupAfterUse(true);
            temp_map->erase(name);
            mp_repository = temp_map;
            status = grpc::Status::OK;

            LOG(INFO) << "Dropped the data structure \'" << name << "\'";
        }
    }
    else
    {
        LOG(WARNING) << "An attempt was made to drop \'" << name << "\' which does not exist";
        status = grpc::Status(grpc::StatusCode::NOT_FOUND, "I could not find the table in the server");
    }

    m_Lock.unlock();

    // only auto save if the request was good
    if (status.ok())
        autoSave();

    return status;
}

grpc::Status sampling_structure_repository::drop_structure(const serverProto::DropRequest& request, serverProto::DropResponse &response)
{
    grpc::Status status;

    std::string structure_to_drop = request.structure_name();

    status = this->drop_structure(structure_to_drop);

    response.set_success(status.ok());
    return status;
}

bool sampling_structure_repository::save_state(std::string outputFile)
{
    ServerState::SamplingStructureRepoDump dump;
    bool ret_val = true;

    // obtain the lock, we we know the data will not change as we package the data
    m_Lock.lock();

    dump.set_time_saved(time(NULL));

    // scan everything and put it in the protobuf
    for (auto i = mp_repository->begin(); i != mp_repository->end(); ++i)
    {
        // if it is a nullptr it means it is currently being built.  We will
        // only save things which have been completely built.
        if (!i->second)
            continue;

        auto d_elm = dump.add_data();

        d_elm->set_name(i->first);
        d_elm->set_filename(i->second->get_BackingFile());
        d_elm->set_type(i->second->getPayloadType());
        d_elm->set_time_built(i->second->get_construction_time());
    }

    m_Lock.unlock();
    // write the data to the specified file
    std::fstream output(outputFile, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!dump.SerializeToOstream(&output))
    {
        LOG(WARNING) << "Unable to write state of the database to " << outputFile;
        ret_val = false;
    }
    else
    {
        LOG(INFO) << "State of the database has been written to " << outputFile;
    }

    return ret_val;
}

void sampling_structure_repository::init_from_protobuf(const ServerState::SamplingStructureRepoDump& state)
{
    // we do not need the write lock.  All of the inserting will be done inside in the
    // function which writes the new value to the data structure

    // for each record in the proto, create a sampling structure and insert it into the registry.
    for (int i = 0; i < state.data_size(); ++i)
    {
        struct stat buf;
        std::string filename_wextention = state.data(i).filename() + ".data";
        if (stat(filename_wextention.c_str(), &buf) == -1)
        {
            LOG(WARNING) << "Unable to open previous sampling file " << filename_wextention << " because it can not be found";
            continue;
        }


        try{
            // create the appropriate sampling structure
            std::shared_ptr<sampling_structure> new_structure;
            switch (state.data(i).type())
            {
            case serverProto::SampleStructureType::NO_PAYLOAD:
                new_structure = RStree_basic::open_RStree_basic(state.data(i).filename(), state.data(i).time_built());
                break;
            case serverProto::SampleStructureType::INT_PAYLOAD:
            case serverProto::SampleStructureType::FLOAT_PAYLOAD:
            default:
                LOG(FATAL) << "Unsupported sampling type requested for initialization";
            }

            // insert that sampling structure in the repository
            auto ret_val = add_new_structure(new_structure, state.data(i).name());

            LOG(INFO) << "The data structure \'" << state.data(i).name() << "\' has been inserted into the repository [size=" << mp_repository->find(state.data(i).name())->second->get_size() << "]";
        }
        catch (...)
        {
            LOG(FATAL) << "Unable to open the file " << state.data(i).filename();
        }
    }

}

bool sampling_structure_repository::set_autosave(bool t)
{
    auto_save = t;
}

bool sampling_structure_repository::autoSave()
{
    if (auto_save)
        return save_state(m_checkpointFile);
    return false;
}