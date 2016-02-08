/**

  UniversQL JSON Adapter
  Copyright 2016 Brandon Carl
  MIT Licensed

**/


//
//  Dependencies
//

_ = require("./lib/lodash.custom");



//
//  exports
//

module.exports = {
  name: "json",
  compile: compile,
  run: run
};



/**

  compile

  @param {Function} templater Inserts variables via basic templating.

**/

function compile(universalQuery, templater) {

  var query = {};

  // Process sorts: sorts are given in {key, val} but need to be in arrays for lodash
  if (universalQuery.filters && universalQuery.filters.length)
    query.filters = templater(universalQuery.filters, mapFilters);

  if (universalQuery.sort && universalQuery.sort.length)
    query.sort = templater(universalQuery.sort, mapSorts);

  if (universalQuery.fields && universalQuery.fields.length)
    query.fields = templater(universalQuery.fields, mapFields);

  if (universalQuery.limit)
    query.limit = templater(universalQuery.limit, mapLimit);

  if (universalQuery.table)
    query.table = templater(universalQuery.table, function(x) { return x; });

  return function(context) {
    return {
      filters : (query.filters) ? query.filters(context) : null,
      sort    : (query.sort) ? query.sort(context) : null,
      fields  : (query.fields) ? query.fields(context) : null,
      limit   : (query.limit) ? query.limit(context) : null,
      table   : (query.table) ? query.table(context) : null
    };
  };

}



/**

  run

**/

function run(query, data, next) {

  var chain, tmp;

  // Use specified table (if available)
  if (query.table && !Array.isArray(data)) data = data[query.table];

  // Create lodash wrapper (unless already created)
  chain = _(data);

  // Ensure we have an array of data
  if (!Array.isArray(data)) throw new Error("Data must be an array")

  // Follow SQL-convention: FROM, WHERE, GROUP BY, HAVING, SELECT, ORDER BY
  // Except that we reverse the final two operations so that we can limit before
  // mapping. This offers a substantial speed improvement.
  if (query.filters)
    chain = chain.filter(query.filters);

  if (query.sort)
    chain = chain.orderBy(query.sort.keys, query.sort.orders);

  if (query.limit)
    chain = (query.limit >= 0) ? chain.take(query.limit) : chain.takeRight(-query.limit);

  // Map last! (most processor intensive)
  if (query.fields)
    chain = chain.map(query.fields);

  next(null, chain.value());

};


function mapSorts(sorts) {

  var hasVars = false,
      keys    = new Array(sorts.length),
      vals    = new Array(sorts.length);

  for (var i = 0, n = sorts.length; i < n; i++) {
    keys[i] = sorts[i].key;
    vals[i] = sorts[i].order;
  }

  return { keys : keys, orders : vals }

}


function mapLimit(limit) {
  return parseInt(limit);
}


function mapFields(fields) {
  var n = fields.length;
  return function(x) {
    var obj = {};
    for (var i = 0; i < n; i++)
      _.set(obj, fields[i], _.get(x, fields[i]));
    return obj;
  }
}


function mapFilters(filters) {

  // Create a string representing the function and then assemble into new function
  var fn = "",
      filter,
      keys  = [],
      vars  = [],
      stack = [];

  // Create filter logic (we use a stack to evaluate the RPN)
  for (var i = 0, n = filters.length; i < n; i++) {
    filter = filters[i];

    // Statement
    if ("object" == typeof filter) {

      // We store mapping between in-function variables ($Vx) and data values
      // in order to create a function
      keys.push(filter.key);
      vars.push("$V" + i);

      if ("~" === filter.comparator)
        fn += "var $" + i + "=" + filter.value + ".test($V" + i + ");"
      else
        fn += "var $" + i + "=($V" + i + (("=" === filter.comparator) ? "==" : filter.comparator) + filter.value + ");";

    // Operator
    } else {

      // We allow "&" or "&&" for AND. Similar for OR. Must convert first.
      if (1 === filter.length) filter = filter + filter;

      if (1 === stack.length)
        fn += "var $" + i + "=" + stack.pop() + ";"
      else
        fn += "var $" + i + "=(" + stack.pop() + filter + stack.pop() + ");"

    }

    stack.push("$" + i);
  }

  fn += "return $" + (i-1) + ";"

  // Overload the function
  fn = Function.apply(null, vars.concat(fn));

  return function(x) {
    var args = _.map(keys, function(key) { return _.get(x, key); });
    return fn.apply(null, args);
  };

}

function isNumeric(x) {
    return !isNaN(parseFloat(x)) && isFinite(x);
}


function inferType(x) {
  if ("true" === x)
    return true;
  else if ("false" === x)
    return false;
  else if (isNumeric(x))
    return parseFloat(x);
  else if ("string" === typeof x)
    return "'" + x + "'";
  else
    return x;
}
