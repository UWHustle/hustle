#ifndef HUSTLE_PARSETREE_H
#define HUSTLE_PARSETREE_H


namespace hustle {
namespace frontend {


class Expr {
public:
  template<class Archive>
  void serialize(Archive & archive)
  {
    archive(
        CEREAL_NVP(type),
        CEREAL_NVP(value),
        CEREAL_NVP(i_table),
        CEREAL_NVP(i_column)
    );
  }

private:
  std::string type;
  int value;
  int i_table;
  int i_column;
};

class Predicate {
public:
  template<class Archive>
  void serialize(Archive & archive)
  {
    archive(
        CEREAL_NVP(left),
        CEREAL_NVP(op),
        CEREAL_NVP(right)
    );
  }

private:
  Expr left;
  int op;
  Expr right;
};

class IndexPredicate {
public:
  template<class Archive>
  void serialize(Archive & archive)
  {
    archive(
        CEREAL_NVP(fromtable),
        CEREAL_NVP(predicates)
    );
  }

private:
  int fromtable;
  std::vector<Predicate> predicates;
};


class ParseTree {
public:
  ParseTree(): project(), index_pred(), other_pred() {}

  template<class Archive>
  void serialize(Archive & archive)
  {
    archive(
        CEREAL_NVP(project),
        CEREAL_NVP(index_pred),
        CEREAL_NVP(other_pred)
        );
  }


private:
  std::vector<std::string> project;
  std::vector<IndexPredicate> index_pred;
  std::vector<Predicate> other_pred;
};

}
}


#endif //HUSTLE_PARSETREE_H
