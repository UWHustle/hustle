// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <queue>

#include "types.h"
#include "table.h"
#include "file.h"
#include "naive_column_block.h"
#include "utility.h"
#include "env.h"

namespace hustle::bitweaving {

/**
 * @brief Structure for private members of Class Table.
 */
    struct BWTable::Rep {
        ~Rep() {
            delete env;
        }

        /**
         * @brief The number of tuples in this table.
         */
        size_t num;
        /**
         * @brief The location of this table in file system.
         */
        std::string path;
        /**
         * @brief The Env object of this table.
         */
        Env *env;
        /**
         * @brief The options to control this table.
         */
        Options options;
        /**
         * @brief A mapping from column names to column objects.
         */
        std::map<std::string, Column *> map;
        /**
         * @brief A list of freed column IDs.
         */
        std::queue<ColumnId> free_column_ids;
    };

    BWTable::BWTable(const std::string &path, const Options &options) {
        rep_ = new Rep();
        rep_->num = 0;
        rep_->env = new Env();
        rep_->options = options;
        if (!options.in_memory)
            rep_->path = path;
    }

    BWTable::~BWTable() {
        delete rep_;
    }

    Status BWTable::Open() {
        Status status;

        if (rep_->options.in_memory)
            return Status::OK();

        status = rep_->env->CreateDirectoryPath(rep_->path);
        if (!status.IsOk()) {
            return status;
        }

        std::string meta_file;
        status = rep_->env->FilePath(rep_->path, "meta.dat", meta_file);
        if (!status.IsOk()) {
            return status;
        }

        if (!rep_->options.delete_exist_files && rep_->env->IsFileExist(meta_file)) {
            //Load data from disk
            return LoadMetaFile(meta_file);
        }
        return Status::OK();
    }

    Status BWTable::Close() {
        if (rep_->options.in_memory)
            return Status::OK();

        return Save();
    }

    Status BWTable::Save() {
        Status status;

        if (rep_->options.in_memory)
            return Status::OK();

        std::string meta_file;
        status = rep_->env->FilePath(rep_->path, "meta.dat", meta_file);
        if (!status.IsOk()) {
            return status;
        }
        status = SaveMetaFile(meta_file);
        if (!status.IsOk())
            return status;

        std::map<std::string, Column *>::iterator iter;
        for (iter = rep_->map.begin(); iter != rep_->map.end(); ++iter) {
            std::string file_path;
            rep_->env->FilePath(rep_->path, iter->first + ".dat", file_path);

            SequentialWriteFile file;
            status = file.Open(file_path);
            if (!status.IsOk()) {
                return status;
            }

            iter->second->Save(file);

            status = file.Flush();
            if (!status.IsOk()) {
                return status;
            }
            status = file.Close();
            if (!status.IsOk()) {
                return status;
            }
        }
        return Status::OK();
    }

    Status BWTable::LoadMetaFile(const std::string &filename) {
        Status status;
        SequentialReadFile file;

        if (rep_->options.in_memory)
            return Status::OK();

        status = file.Open(filename);
        if (!status.IsOk())
            return status;

        size_t str_len;
        status = file.Read(reinterpret_cast<char *>(&str_len), sizeof(size_t));
        if (!status.IsOk())
            return status;

        char *buffer = new char[str_len + 1];
        status = file.Read(buffer, str_len);
        if (!status.IsOk())
            return status;
        buffer[str_len] = '\0';

        std::string str(buffer);
        std::istringstream ss(str);
        ss >> rep_->num;
        size_t num_columns;
        ss >> num_columns;
        for (size_t i = 0; i < num_columns; i++) {
            ColumnType type;
            uint8_t bit_width;
            std::string name;
            ss >> name >> type >> bit_width;
            AddColumn(name, type, bit_width);
        }

        status = file.Close();
        if (!status.IsOk())
            return status;

        return Status::OK();
    }

    Status BWTable::SaveMetaFile(const std::string &filename) {
        Status status;
        SequentialWriteFile file;

        if (rep_->options.in_memory)
            return Status::OK();

        status = file.Open(filename);
        if (!status.IsOk())
            return status;

        std::ostringstream converter;
        converter << rep_->num << " ";
        converter << rep_->map.size() << " ";
        std::map<std::string, Column *>::iterator iter;
        for (iter = rep_->map.begin(); iter != rep_->map.end(); ++iter) {
            converter << iter->first << " ";
            converter << iter->second->GetType() << " ";
            converter << iter->second->GetBitWidth() << " ";
        }
        std::string str = converter.str();
        size_t str_len = str.length();
        status = file.Append(reinterpret_cast<char *>(&str_len), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Append(str.c_str(), str_len);
        if (!status.IsOk())
            return status;

        status = file.Flush();
        if (!status.IsOk())
            return status;

        status = file.Close();
        if (!status.IsOk())
            return status;

        return Status::OK();
    }

    BitVector *BWTable::CreateBitVector() const {
        return new BitVector(rep_->num);
    }

    Iterator *BWTable::CreateIterator() const {
        BitVector *bitvector = CreateBitVector();
        bitvector->SetOnes();
        return new Iterator(*this, *bitvector);
    }

    Iterator *BWTable::CreateIterator(const BitVector &bitvector) const {
        return new Iterator(*this, bitvector);
    }

    Status BWTable::AddColumn(const std::string &name, ColumnType type, size_t bit_width) {
        ColumnId column_id;
        if (rep_->free_column_ids.size() > 0) {
            column_id = rep_->free_column_ids.front();
        } else {
            column_id = rep_->map.size();
        }
        Column *column = new Column(column_id, type, bit_width, rep_->num);
        std::pair<std::map<std::string, Column *>::iterator, bool> ret;
        ret = rep_->map.insert(std::pair<std::string, Column *>(name, column));
        if (ret.second == false) {
            return Status::UsageError("The column name exists in the table.");
        }
        if (rep_->free_column_ids.size() > 0) {
            rep_->free_column_ids.pop();
        }
        return Status::OK();
    }

    Status BWTable::RemoveColumn(const std::string &name) {
        std::map<std::string, Column *>::iterator iter;
        iter = rep_->map.find(name);
        if (iter != rep_->map.end()) {
            // Add column id into free list
            rep_->free_column_ids.push(iter->second->GetColumnId());
            rep_->map.erase(iter);
        } else {
            return Status::UsageError("The column name doesn't exist.");
        }
        return Status::OK();
    }

    Column *BWTable::GetColumn(const std::string &name) const {
        std::map<std::string, Column *>::iterator iter;
        iter = rep_->map.find(name);
        if (iter != rep_->map.end()) {
            return iter->second;
        }
        return NULL;
    }

    ColumnId BWTable::GetMaxColumnId() const {
        ColumnId max_column_id = rep_->map.size() + rep_->free_column_ids.size();
        return max_column_id;
    }

    Column *BWTable::GetColumn(ColumnId column_id) const {
        if (column_id < GetMaxColumnId()) {
            std::map<std::string, Column *>::iterator iter;
            for (iter = rep_->map.begin(); iter != rep_->map.end(); ++iter) {
                if (iter->second->GetColumnId() == column_id)
                    return iter->second;
            }
        }
        return NULL;
    }

    size_t BWTable::GetNumRows() {
        return rep_->num;
    }

    Code *BWTable::GetColumnCodesThusFar(Column *old_col, size_t old_col_size) {
        Code *codes = new bitweaving::Code[old_col_size];
        Status status;
        for (size_t i = 0; i < old_col_size; i++) {
            status = old_col->GetCode(i, codes[i]);
            assert(status.IsOk());
        }
        return codes;
    }

    Column *BWTable::RemoveAndAddColumn(Column *old_col, const std::string &name, size_t old_col_size, size_t new_bitwidth) {

        Code *old_codes = nullptr;
        if(old_col_size != 0){ // Else, this is the first set of codes we are appending to the column. No copy required
            old_codes = GetColumnCodesThusFar(old_col, old_col_size);
        }
        ColumnType type = old_col->GetType();
        BWTable::RemoveColumn(name);
        AddColumn(name, type, new_bitwidth);
        Column *new_col = GetColumn(name);
        if(old_codes != nullptr){
            AppendResult* result = new_col->Append(old_codes, old_col_size);
            assert(result->GetAppendStatus().IsOk()); // This must ideally be successful as it was
            // already successful even with the previous bitwidth
        }

        return new_col;
    }

    AppendResult* BWTable::AppendToColumn(const std::string &name, Code *codes, size_t num) {
        Column *col = GetColumn(name);
        AppendResult *append_result = col->Append(codes, num);
        while (!append_result->DoesCodeFitInBitwidth()) {
            //Bitwidth is not optimum. So remove the column and add one again by increasing the bitwidth
            // to the suggested one and copy all the codes from the old column
            Column* new_col = RemoveAndAddColumn(col, name, col->GetNumValuesInColumn(), append_result->GetSuggestedBitwidth());
            append_result = new_col->Append(codes, num);
        }
        //Check the max value of the column. If the bitwidth used it much more than the max value, optimize it
        // Ideally this must not result in InvalidBitWidth status error.
        size_t curr_bitwidth = append_result->GetSuggestedBitwidth();
        size_t optimum_bitwidth = ceil(log2(col->GetMaxCode()));
        //std::cout << "Max value bitwidth " << (uint32_t) optimum_bitwidth << std::endl;
        if (curr_bitwidth != optimum_bitwidth) {
            col = GetColumn(name); //Get the updated col object; Might have been updated if the bitwidth was
            // initially small
            int old_codes = col->GetNumValuesInColumn() - num;
            Column* new_col = RemoveAndAddColumn(col, name, old_codes, optimum_bitwidth);
            append_result = new_col->Append(codes, num);
            assert(append_result->GetAppendStatus().IsOk());
        }

        return new AppendResult(Status::OK(), true, append_result->GetSuggestedBitwidth());
    }


} // namespace bitweaving

