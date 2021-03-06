/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.parser.v1_8

import org.neo4j.cypher.internal.commands._

trait Expressions extends Base with ParserPattern with Predicates {
  def expression: Parser[Expression] = term ~ rep("+" ~ term | "-" ~ term) ^^ {
    case head ~ rest =>
      var result = head
      rest.foreach {
        case "+" ~ f => result = Add(result, f)
        case "-" ~ f => result = Subtract(result, f)
      }

    result
  }

  def term: Parser[Expression] = factor ~ rep("*" ~ factor | "/" ~ factor | "%" ~ factor | "^" ~ factor) ^^ {
    case head ~ rest =>
      var result = head
      rest.foreach {
        case "*" ~ f => result = Multiply(result, f)
        case "/" ~ f => result = Divide(result, f)
        case "%" ~ f => result = Modulo(result, f)
        case "^" ~ f => result = Pow(result, f)
      }

      result
  }

  def factor: Parser[Expression] =
  (ignoreCase("true") ^^^ Literal(true)
      | ignoreCase("false") ^^^ Literal(false)
      | ignoreCase("null") ^^^ Literal(null)
      | pathExpression
      | extract
      | function
      | aggregateExpression
      | coalesceFunc
      | filterFunc
      | nullableProperty
      | property
      | stringLit
      | numberLiteral
      | collectionLiteral
      | parameter
      | entity
      | parens(expression)
      | failure("illegal start of value"))

  def stringLit: Parser[Expression] = Parser {
    case in if in.atEnd => Failure("out of string", in)
    case in =>
      val start = handleWhiteSpace(in.source, in.offset)
      val string = in.source.subSequence(start, in.source.length()).toString
      val startChar = string.charAt(0)
      if (startChar != '\"' && startChar != '\'')
        Failure("expected string", in)
      else {

        var ls = string.toList.tail
        val sb = new StringBuilder(ls.length)
        var idx = start
        var result: Option[ParseResult[Expression]] = None

        while (!ls.isEmpty && result.isEmpty) {
          val (pref, suf) = ls span { c => c != '\\' && c != startChar }
          idx += pref.length
          sb ++= pref

          if (suf.isEmpty)
            result = Some(Failure("end of string missing", in))

          val first: Char = suf(0)
          first match {
            case c if c == startChar         =>
              result = Some(Success(Literal(sb.result()), in.drop(idx - in.offset + 2)))
            case '\\' if suf(1) == '\''||suf(1)=='\"' =>
              sb.append(suf(1))
              idx += 2
              ls = suf.drop(2)
          }
        }

        result match {
          case Some(x) => x
          case None    => Failure("end of string missing", in)
        }
      }
  }

  def numberLiteral: Parser[Expression] = number ^^ (x => {
    val value: Any = if (x.contains("."))
      x.toDouble
    else
      x.toLong

    Literal(value)
  })

  def entity: Parser[Entity] = identity ^^ (x => Entity(x))

  def collectionLiteral: Parser[Expression] = "[" ~> repsep(expression, ",") <~ "]" ^^ (seq => Collection(seq: _*))

  def property: Parser[Expression] = identity ~ "." ~ identity ^^ {
    case v ~ "." ~ p => createProperty(v, p)
  }

  def createProperty(entity: String, propName: String): Expression

  def nullableProperty: Parser[Expression] = (
    property <~ "?" ^^ (p => new Nullable(p) with DefaultTrue) |
      property <~ "!" ^^ (p => new Nullable(p) with DefaultFalse))

  def extract: Parser[Expression] = ignoreCase("extract") ~> parens(identity ~ ignoreCase("in") ~ expression ~ ":" ~ expression) ^^ {
    case (id ~ in ~ iter ~ ":" ~ expression) => ExtractFunction(iter, id, expression)
  }

  def coalesceFunc: Parser[Expression] = ignoreCase("coalesce") ~> parens(commaList(expression)) ^^ {
    case expressions => CoalesceFunction(expressions: _*)
  }

  def filterFunc: Parser[Expression] = ignoreCase("filter") ~> parens(identity ~ ignoreCase("in") ~ expression ~ (ignoreCase("where") | ":") ~ predicate) ^^ {
    case symbol ~ in ~ collection ~ where ~ pred => FilterFunction(collection, symbol, pred)
  }

  def function: Parser[Expression] = Parser {
    case in => {
      val inner = identity ~ parens(commaList(expression | entity))

      inner(in) match {

        case Success(name ~ args, rest) => functions.get(name.toLowerCase) match {
          case None => failure("unknown function", rest)
          case Some(func) if !func.acceptsTheseManyArguments(args.size) => failure("Wrong number of parameters for function " + name, rest)
          case Some(func) => Success(func.create(args), rest)
        }

        case Failure(msg, rest) => Failure(msg, rest)
        case Error(msg, rest) => Error(msg, rest)
      }
    }
  }


  private def func(numberOfArguments: Int, create: List[Expression] => Expression) = new Function(x => x == numberOfArguments, create)

  case class Function(acceptsTheseManyArguments: Int => Boolean, create: List[Expression] => Expression)

  val functions = Map(
    "type" -> func(1, args => RelationshipTypeFunction(args.head)),
    "id" -> func(1, args => IdFunction(args.head)),
    "length" -> func(1, args => LengthFunction(args.head)),
    "nodes" -> func(1, args => NodesFunction(args.head)),
    "rels" -> func(1, args => RelationshipFunction(args.head)),
    "relationships" -> func(1, args => RelationshipFunction(args.head)),
    "abs" -> func(1, args => AbsFunction(args.head)),
    "round" -> func(1, args => RoundFunction(args.head)),
    "sqrt" -> func(1, args => SqrtFunction(args.head)),
    "sign" -> func(1, args => SignFunction(args.head)),
    "head" -> func(1, args => HeadFunction(args.head)),
    "last" -> func(1, args => LastFunction(args.head)),
    "tail" -> func(1, args => TailFunction(args.head)),
    "shortestpath" -> Function(x => false, args => null),
    "range" -> Function(x => x == 2 || x == 3, args => {
      val step = if (args.size == 2) Literal(1) else args(2)
      RangeFunction(args(0), args(1), step)
    })
  )

  def aggregateExpression: Parser[Expression] = countStar | aggregationFunction

  def aggregateFunctionNames: Parser[String] = ignoreCases("count", "sum", "min", "max", "avg", "collect")

  def aggregationFunction: Parser[Expression] = aggregateFunctionNames ~ parens(opt(ignoreCase("distinct")) ~ expression) ^^ {
    case function ~ (distinct ~ inner) => {

      val aggregateExpression = function match {
        case "count" => Count(inner)
        case "sum" => Sum(inner)
        case "min" => Min(inner)
        case "max" => Max(inner)
        case "avg" => Avg(inner)
        case "collect" => Collect(inner)
      }

      if (distinct.isEmpty) {
        aggregateExpression
      }
      else {
        Distinct(aggregateExpression, inner)
      }
    }
  }

  def countStar: Parser[Expression] = ignoreCase("count") ~> parens("*") ^^^ CountStar()

  def pathExpression: Parser[Expression] = usePath(translate) ^^ {//(pathPattern => PathExpression(pathPattern))
    case Seq(x:ShortestPath) => ShortestPathExpression(x)
    case patterns => PathExpression(patterns)
  }

  private def translate(abstractPattern: AbstractPattern): Maybe[Pattern] = matchTranslator(abstractPattern) match {
      case Yes(Seq(np)) if np.isInstanceOf[NamedPath] => No(Seq("Can't assign to an identifier in a pattern expression"))
      case Yes(p@Seq(pattern:Pattern)) => Yes(p.asInstanceOf[Seq[Pattern]])
      case n: No => n
    }

  def matchTranslator(abstractPattern: AbstractPattern): Maybe[Any]
}

trait DefaultTrue

trait DefaultFalse












