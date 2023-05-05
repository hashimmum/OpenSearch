/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

/**
 * An interface representing a user provider
 */
public interface UserProvider {

    /**
     * A method that is able to return a specific user with the matching username
     * @param username
     * @return
     */
    public User getUser(String username);

    public void removeUser(String username);

    public void putUser(ObjectNode userContent);

    public List<User> getUsers();
}
