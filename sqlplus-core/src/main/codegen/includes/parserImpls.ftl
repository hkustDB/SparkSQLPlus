<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

boolean IfNotExistsOpt() :
{
}
{
    (
        LOOKAHEAD(3)
        <IF> <NOT> <EXISTS> { return true; }
    |
        { return false; }
    )
}

void TableColumn(TableCreationContext context) :
{
    SqlTableConstraint constraint;
}
{
    (
        LOOKAHEAD(2)
        TypedColumn(context)
    |
        constraint = TableConstraint() {
            context.constraints.add(constraint);
        }
    )
}

void TypedColumn(TableCreationContext context) :
{
    SqlIdentifier name;
    SqlParserPos pos;
    SqlDataTypeSpec type;
    SqlTypeNameSpec typeName;
}
{
    name = SimpleIdentifier() {pos = getPos();}
    typeName = TypeName()
    {
        type = new SqlDataTypeSpec(typeName, getPos());
        SqlTableColumn regularColumn = new SqlTableColumn(
            getPos(),
            name,
            type);
        context.columnList.add(regularColumn);
    }
}

SqlNodeList TableProperties():
{
    SqlNode property;
    final List<SqlNode> proList = new ArrayList<SqlNode>();
    final Span span;
}
{
    <LPAREN> { span = span(); }
    [
        property = TableOption()
        {
            proList.add(property);
        }
        (
            <COMMA> property = TableOption()
            {
                proList.add(property);
            }
        )*
    ]
    <RPAREN>
    {  return new SqlNodeList(proList, span.end(this)); }
}

SqlNode TableOption() :
{
    SqlNode key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = StringLiteral()
    { pos = getPos(); }
    <EQ> value = StringLiteral()
    {
        return new SqlTableOption(key, value, getPos());
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlParserPos startPos = s.pos();
    boolean ifNotExists = false;
    SqlIdentifier tableName;
    SqlNodeList columnList = SqlNodeList.EMPTY;
    List<SqlTableConstraint> constraints = new ArrayList<SqlTableConstraint>();
    SqlNodeList propertyList = SqlNodeList.EMPTY;
    SqlParserPos pos = startPos;
}
{
    <TABLE>

    ifNotExists = IfNotExistsOpt()

    tableName = CompoundIdentifier()
    [
        <LPAREN> { pos = getPos(); TableCreationContext ctx = new TableCreationContext();}
        TableColumn(ctx)
        (
            <COMMA> TableColumn(ctx)
        )*
        {
            pos = pos.plus(getPos());
            columnList = new SqlNodeList(ctx.columnList, pos);
            constraints = ctx.constraints;
        }
        <RPAREN>
    ]
    [
        <WITH>
        propertyList = TableProperties()
    ]
    {
        return new SqlCreateTable(startPos.plus(getPos()),
                tableName,
                columnList,
                constraints,
                propertyList,
                ifNotExists);
    }
}

/** Parses a table constraint for CREATE TABLE. */
SqlTableConstraint TableConstraint() :
{
    SqlIdentifier constraintName = null;
    final SqlLiteral uniqueSpec;
    final SqlNodeList columns;
}
{
    [ constraintName = ConstraintName() ]
    uniqueSpec = UniqueSpec()
    columns = ParenthesizedSimpleIdentifierList()
    {
        return new SqlTableConstraint(
        constraintName,
        uniqueSpec,
        columns,
        getPos());
    }
}

SqlIdentifier ConstraintName() :
{
    SqlIdentifier constraintName;
}
{
    <CONSTRAINT> constraintName = SimpleIdentifier() {
        return constraintName;
    }
}

SqlLiteral UniqueSpec() :
{
    SqlLiteral uniqueSpec;
}
{
    (
        <PRIMARY> <KEY> {
            uniqueSpec = SqlUniqueSpec.PRIMARY_KEY.symbol(getPos());
        }
    )
    {
        return uniqueSpec;
    }
}