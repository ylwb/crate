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

package io.crate.sql;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.FloatLiteral;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.ShortLiteral;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class NumericLiteralsTest {

    @Test
    public void test_short_literal() {
        var expr = SqlParser.createExpression(Short.toString(Short.MAX_VALUE));
        assertThat(expr, instanceOf(ShortLiteral.class));
    }

    @Test
    public void test_integer_literal() {
        var expr = SqlParser.createExpression(Integer.toString(Integer.MAX_VALUE));
        assertThat(expr, instanceOf(IntegerLiteral.class));
    }

    @Test
    public void test_long_literal() {
        var expr = SqlParser.createExpression(Long.toString(Integer.MAX_VALUE + 1L));
        assertThat(expr, instanceOf(LongLiteral.class));
    }

    @Test
    public void test_float_literal() {
        var expr = SqlParser.createExpression("12345.123");
        assertThat(((FloatLiteral) expr).getValue(), is(12345.123f));
    }

    @Test
    public void test_double_literal() {
        var expr = SqlParser.createExpression("12345.1234");
        assertThat(((DoubleLiteral) expr).getValue(), is(12345.1234d));
    }
}
