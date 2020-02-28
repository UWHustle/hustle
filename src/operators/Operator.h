#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <string>
#include <table/block.h>
#include <table/table.h>

namespace hustle {
namespace operators {

enum FilterOperator {
    AND,
    OR
};

class Operator{
public:
    // TODO(nicholas): abide by naming conventions
    virtual std::shared_ptr<Table> runOperator
        (std::vector<std::shared_ptr<Table>> tables) = 0;

//    virtual void set_children(std::shared_ptr<Operator> left_child,
//            std::shared_ptr<Operator> right_child) = 0;
protected:
    std::shared_ptr<Operator> left_child_;
    std::shared_ptr<Operator> right_child_;
    FilterOperator filter_operator_;
};

//class OperatorLeaf : public Operator {
//    public:
//
//    virtual std::shared_ptr<Table> runOperator
//            (std::vector<std::shared_ptr<Table>> tables) = 0;
//
//};
//
//class OperatorComposite : public Operator {
//    public:
//
//    std::shared_ptr<Operator> left_child;
//    std::shared_ptr<Operator> right_child;
//};


} // namespace operators
} // namespace hustle

#endif //HUSTLE_OPERATOR_H
