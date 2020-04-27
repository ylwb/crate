/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.operator.any;

import io.crate.expression.operator.Operator;
import io.crate.expression.operator.OperatorModule;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.function.IntPredicate;

import static io.crate.expression.operator.any.AnyOperator.OPERATOR_PREFIX;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public final class AnyOperators {

    public static class Names {
        public static final String EQ = Type.EQ.fullQualifiedName;
        public static final String GTE = Type.GTE.fullQualifiedName;
        public static final String GT = Type.GT.fullQualifiedName;
        public static final String LTE = Type.LTE.fullQualifiedName;
        public static final String LT = Type.LT.fullQualifiedName;
        public static final String NEQ = Type.NEQ.fullQualifiedName;
    }

    private enum Type {
        EQ(ComparisonExpression.Type.EQUAL, result -> result == 0),
        NEQ(ComparisonExpression.Type.NOT_EQUAL, result -> result != 0),
        GTE(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, result -> result >= 0),
        GT(ComparisonExpression.Type.GREATER_THAN, result -> result > 0),
        LTE(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, result -> result <= 0),
        LT(ComparisonExpression.Type.LESS_THAN, result -> result < 0);

        final String fullQualifiedName;
        final IntPredicate cmp;

        Type(ComparisonExpression.Type type, IntPredicate cmp) {
            this.fullQualifiedName = OPERATOR_PREFIX + type.getValue();
            this.cmp = cmp;
        }
    }

    public static void register(OperatorModule module) {
        for (var type : Type.values()) {
            module.register(
                Signature.scalar(
                    type.fullQualifiedName,
                    parseTypeSignature("E"),
                    parseTypeSignature("array(E)"),
                    Operator.RETURN_TYPE.getTypeSignature()
                ).withTypeVariableConstraints(typeVariable("E")),
                (signature, dataTypes) ->
                    new AnyOperator(
                        new FunctionInfo(new FunctionIdent(signature.getName().name(), dataTypes), DataTypes.BOOLEAN),
                        signature,
                        type.cmp
                    )
            );
        }
    }

    public static Iterable<?> collectionValueToIterable(Object collectionRef) throws IllegalArgumentException {
        if (collectionRef instanceof Object[]) {
            return Arrays.asList((Object[]) collectionRef);
        } else if (collectionRef instanceof Collection) {
            return (Collection<?>) collectionRef;
        } else {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "cannot cast %s to Iterable", collectionRef));
        }
    }

    private AnyOperators() {
    }
}
