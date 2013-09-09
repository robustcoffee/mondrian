/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.regex.*;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Ads database.
 * AdsDialect is based on the implementation of OracleDialect with changes on 
 * @author jhyde
 * @version $Id: //open/mondrian/src/main/mondrian/spi/impl/AdsDialect.java#14 $
 * @since Nov 23, 2008
 */
public class AdsDialect extends JdbcDialectImpl {

    private final String flagsRegexp = "^(\\(\\?([a-zA-Z])\\)).*$";
    private final Pattern flagsPattern = Pattern.compile(flagsRegexp);
    private final String escapeRegexp = "^.*(\\\\Q(.*)\\\\E).*$";
    private final Pattern escapePattern = Pattern.compile(escapeRegexp);

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            AdsDialect.class,
            DatabaseProduct.ADS);

    /**
     * Creates an AdsDialect.
     *
     * @param connection Connection
     */
    public AdsDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean allowsAs() {
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList,
            " from dual", false);
    }

    public boolean supportsGroupingSets() {
        return true;
    }
	
	@Override
    public boolean allowsFromQuery() {
        return false;
    }

    @Override
    public boolean allowsJoinOn() {
        return true;
    }


    @Override
    public boolean requiresGroupByAlias() {
        return true;
    }
	
    @Override
    public boolean requiresOrderByAlias() {
        return true;
    }
	
    @Override
    public boolean requiresHavingAlias() {
        return true;
    }
	
    @Override
    public boolean allowsOrderByAlias() {
        return requiresOrderByAlias();
    }

    @Override
    public boolean allowsRegularExpressionInWhereClause() {
        return true;
    }
	

    @Override
    public String generateRegularExpression(
        String source,
        String javaRegex)
    {
        try {
            Pattern.compile(javaRegex);
        } catch (PatternSyntaxException e) {
            // Not a valid Java regex. Too risky to continue.
            return null;
        }
        final Matcher flagsMatcher = flagsPattern.matcher(javaRegex);
        final String suffix;
        if (flagsMatcher.matches()) {
            // We need to convert leading flags into Ads
            // specific flags
            final StringBuilder suffixSb = new StringBuilder();
            final String flags = flagsMatcher.group(2);
            if (flags.contains("i")) {
                suffixSb.append("i");
            }
            if (flags.contains("c")) {
                suffixSb.append("c");
            }
            if (flags.contains("m")) {
                suffixSb.append("m");
            }
            suffix = suffixSb.toString();
            javaRegex =
                javaRegex.substring(0, flagsMatcher.start(1))
                + javaRegex.substring(flagsMatcher.end(1));
        } else {
            suffix = "";
        }
        final Matcher escapeMatcher = escapePattern.matcher(javaRegex);
        if (escapeMatcher.matches()) {
            // We need to convert escape characters \E and \Q into
            // Ads compatible escapes.
            String sequence = escapeMatcher.group(2);
            sequence = sequence.replaceAll("\\\\", "\\\\");
            javaRegex =
                javaRegex.replace(
                    escapeMatcher.group(1),
                    sequence);
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("REGEXP_LIKE(");
        sb.append(source);
        sb.append(", ");
        quoteStringLiteral(sb, javaRegex);
        sb.append(", ");
        quoteStringLiteral(sb, suffix);
        sb.append(")");
        return sb.toString();
    }
	
	
	@Override
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
            return expr + (ascending ? " ASC" : " DESC");
        
    }
   protected String deduceIdentifierQuoteString(
            DatabaseMetaData databaseMetaData)
        {
            String quoteIdentifierString =
                super.deduceIdentifierQuoteString(databaseMetaData);

            if (quoteIdentifierString == null) {
                // mm.mysql.2.0.4 driver lies. We know better.
                quoteIdentifierString = "'";
            }
            return quoteIdentifierString;
        }

    public void quoteDateLiteral(StringBuilder buf, String value) {
        Date date;
        try {
            /*
             * The format of the 'value' parameter is not certain.
             * Some JDBC drivers will return a timestamp even though
             * we ask for a date (Ads is one of them). We must try to
             * convert both formats.
             */
            date = Date.valueOf(value);
        } catch (IllegalArgumentException ex) {
            try {
                date =
                    new Date(Timestamp.valueOf(value).getTime());
            } catch (IllegalArgumentException ex2) {
                throw new NumberFormatException(
                    "Illegal DATE literal:  " + value);
            }
        }
        quoteDateLiteral(buf, value, date);
    }
	
}

// End AdsDialect.java
