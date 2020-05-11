//
// Created by Nicholas Corrado on 4/26/20.
//

#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <cstdlib>
#include "OperatorResult.h"
#include "scheduler/Task.hpp"

namespace hustle::operators {

    class Operator {

    public:
        virtual std::shared_ptr<OperatorResult> run(Task *ctx) = 0;

        std::size_t getOperatorIndex() const {
            return op_index_;
        }

        void setOperatorIndex(const std::size_t op_index) {
            op_index_ = op_index;
        }

        std::size_t getQueryId() const {
            return query_id_;
        }
        std::shared_ptr<OperatorResult> result_;

        //TODO(nicholas): Make private
        Task* createTask() {
            return CreateLambdaTask([this](Task *ctx) {
                ctx->setTaskType(TaskType::kRelationalOperator);
                ctx->setProfiling(true);
                ctx->setCascade(true);
                ctx->setTaskMajorId(query_id_);

                result_ = this->run(ctx);
            });
        }

    protected:
        explicit Operator(const std::size_t query_id)
                : query_id_(query_id) {}

        const std::size_t query_id_;


    private:


        std::size_t op_index_;

        DISALLOW_COPY_AND_ASSIGN(Operator);

    };
};




#endif //HUSTLE_OPERATOR_H
