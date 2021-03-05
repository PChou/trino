package io.trino.sql.analyzer;

import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LogicalBinaryExpression;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SingleColumn;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IdentityRewriteExpressionVisitor
        extends AstVisitor<Expression, IdentityRewriteExpressionVisitor.IdentityMap>
{
    @Override
    protected Expression visitExpression(Expression node, IdentityMap context)
    {
        return node;
    }

    @Override
    protected Expression visitIdentifier(Identifier node, IdentityMap context)
    {
        return context.candidates.getOrDefault(Optional.of(node), node);
    }

    @Override
    protected Expression visitDereferenceExpression(DereferenceExpression node, IdentityMap context)
    {
        return visitIdentifier(node.getField(), context);
    }

    @Override
    protected Expression visitLogicalBinaryExpression(LogicalBinaryExpression node, IdentityMap context)
    {
        return new LogicalBinaryExpression(node.getOperator(),
                process(node.getLeft(), context),
                process(node.getRight(), context));
    }

    @Override
    protected Expression visitComparisonExpression(ComparisonExpression node, IdentityMap context)
    {
        return new ComparisonExpression(node.getOperator(),
                process(node.getLeft(), context),
                process(node.getRight(), context));
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, IdentityMap context)
    {
        return new FunctionCall(
                node.getLocation(),
                node.getName(),
                node.getWindow(),
                node.getFilter().isPresent() ? Optional.of(process(node.getFilter().get(), context)) : node.getFilter(),
                node.getOrderBy(),
                node.isDistinct(),
                node.getNullTreatment(),
                node.getArguments().stream().map(a -> process(a, context)).collect(Collectors.toList()));
    }

    @Override
    protected Expression visitCast(Cast node, IdentityMap context)
    {
        return new Cast(process(node.getExpression(), context),
                node.getType(),
                node.isSafe(),
                node.isTypeOnly());
    }

    public static class IdentityMap {
        final Map<Optional<Identifier>, Expression> candidates;

        IdentityMap(Map<Optional<Identifier>, Expression> candidates) {
            this.candidates = candidates;
        }

        public static IdentityMap fromQuery(QuerySpecification node) {
            final Map<Optional<Identifier>, Expression> selectAliasMapping = node.getSelect().getSelectItems().stream()
                    .filter(input -> input instanceof SingleColumn)
                    .map(SingleColumn.class::cast)
                    .filter(sc -> sc.getAlias().isPresent())
                    .collect(Collectors.toMap(SingleColumn::getAlias, SingleColumn::getExpression));
            return new IdentityMap(selectAliasMapping);
        }
    }
}
