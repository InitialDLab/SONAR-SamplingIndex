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
#pragma once
// This class is a repository for several sampling structures.  The
// structure is identified by its string name

#include <memory>
#include <string>
#include <list>
#include <map>
#include <mutex>

#include "server_code/sampling_structure.h"

#include "server_code/protobuf/sampling_api.grpc.pb.h"
#include "server_code/protobuf/sampling_api.pb.h"
#include "server_code/protobuf/server_state.pb.h"


class sampling_structure_repository
{
public:
    // create an empty repository, ready for new sampling structures to be added.
    sampling_structure_repository(bool restore_checkpoint = false);

    virtual ~sampling_structure_repository();

    // build and insert the structure described in the BuildRequest
    grpc::Status add_new_structure(const serverProto::BuildRequest &new_structure_information, serverProto::BuildResponse *response = NULL);

    // list the names of all the structures stored in this repository
    void list_structures_proto(serverProto::ListStructuresResponse& response) const;

    // get a pointer to a particular structure
    std::shared_ptr<sampling_structure> get_structure(std::string name);

    // drop a particular structure from this repository
    // returns true if it was removed.
    grpc::Status drop_structure(std::string name);
    grpc::Status drop_structure(const serverProto::DropRequest& request, serverProto::DropResponse &response);

    // dump the state of the server so it can be opened in the future when launching the server
    bool save_state(std::string outputFile);

    // if autosave is true, it will save the state of
    // the repository when a change is made to the repository
    // (after an insert or erase)
    bool set_autosave(bool);

private:
    // insert the structure pointed to by the shared pointer.  Give it
    // the name provided for future reference
    grpc::Status add_new_structure(std::shared_ptr<sampling_structure> new_structure, std::string name);

    void init_from_protobuf(const ServerState::SamplingStructureRepoDump& state);
    
    bool autoSave();

    mutable std::recursive_mutex m_Lock;

    typedef std::map<std::string, std::shared_ptr<sampling_structure> > map_t;

    std::shared_ptr<map_t> mp_repository;

    bool auto_save;

    std::string m_checkpointFile;
};
