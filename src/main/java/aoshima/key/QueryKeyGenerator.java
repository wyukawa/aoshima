package aoshima.key;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Optional;

@Component
public class QueryKeyGenerator implements KeyGenerator {

    @Override
    public Object generate(Object target, Method method, Object... params) {

        if (params.length == 1) {
            if (params[0] instanceof String) {
                String query = (String) params[0];

                SqlParser sqlParser = new SqlParser();
                Statement statement = sqlParser.createStatement(query);
                String formattedQuery = SqlFormatter.formatSql(statement, Optional.empty());

                return formattedQuery;
            }
        }

        throw new RuntimeException("should not reach");
    }
}
