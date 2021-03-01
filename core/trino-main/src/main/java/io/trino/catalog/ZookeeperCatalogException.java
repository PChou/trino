/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.catalog;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import static java.lang.String.format;

public class ZookeeperCatalogException
        extends TrinoException
{

    private static final long serialVersionUID = -6662948452665096247L;

    public static ZookeeperCatalogException newInstance(String message, Object... params) {
        return new ZookeeperCatalogException(format(message, params));
    }

    public static ZookeeperCatalogException newInstance(String message, Throwable e) {
        return new ZookeeperCatalogException(message, e);
    }

    private ZookeeperCatalogException(String message) {
        super(() -> new ErrorCode(20000, "ZookeeperCatalog", ErrorType.INTERNAL_ERROR), message);
    }

    private ZookeeperCatalogException(String message, Throwable e) {
        super(() -> new ErrorCode(20000, "ZookeeperCatalog", ErrorType.INTERNAL_ERROR), message, e);
    }
}
