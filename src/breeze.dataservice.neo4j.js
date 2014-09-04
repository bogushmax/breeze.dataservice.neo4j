(function (definition, window) {
    if (window.breeze) {
        definition(window.breeze);
    } else if (typeof require === "function" && typeof exports === "object" && typeof module === "object") {
        // CommonJS or Node
        var b = require('breeze');
        definition(b);
    } else if (typeof define === "function" && define["amd"] && !window.breeze) {
        // Requirejs / AMD 
        define(['breeze'], definition);
    } else {
        throw new Error("Can't find breeze");
    }
}(function (breeze) {
    var newLine = function (directive) { return '\r\n' + directive; };

    var getEntityType = function (mappingContext, entityName) {
        var metadataStore = mappingContext.metadataStore;
        var query = mappingContext.query;
        var entityName = entityName || metadataStore.getEntityTypeNameForResourceName(query.resourceName);
        var entityType = metadataStore.getEntityType(entityName);
        return entityType;
    };

    /*
        Return array of statements results
    */
    var cypherResultsExtract = function (json) {
        return json.results.map(function (statementResult) {
            var columns = statementResult.columns;
            return statementResult.data.map(function (values) {
                var node = {};
                columns.forEach(function (column, i) {
                    // Transaction result mode: 'row'
                    node[column] = values.row[i];
                });
                return node;
            });
        });
    };

    var JsonResultsAdapter         = breeze.JsonResultsAdapter;
    var AbstractDataServiceAdapter = breeze.AbstractDataServiceAdapter;

    var Neo4jDataService = function () {
        this.initialize();
        this.name = 'neo4j';
        this.ajaxImpl = breeze.config.getAdapterInstance("ajax");
    };

    Neo4jDataService.prototype = new AbstractDataServiceAdapter();

    breeze.core.extend(Neo4jDataService.prototype, {
        buildEntityTypeCypherReturn: function (entityType) {
            var cypherQuery = 'RETURN ';
            cypherQuery += entityType.dataProperties.map(function (property) {
                return 'n.`' + property.nameOnServer + '` AS `' + property.name + '`';
            }).join(',');
            return cypherQuery;
        },

        buildCypherQuery: function (mappingContext) {
            var entityType = getEntityType(mappingContext);
            var entityQuery = mappingContext.query;
            var cypherQuery = 'CYPHER 2.0';
            // Building
            cypherQuery += newLine('MATCH (n:`' + entityType.shortName + '`)');
            // Building navigation properties neo4j matchs
            entityType.navigationProperties.forEach(function (property, index) {
                cypherQuery += newLine('OPTIONAL MATCH (n)-[`' + property.associationName + '`]-(n' + index + ':`' + property.entityType.shortName + '`)');
            });
            // Building neo4j to breeze.js data properties mapper
            cypherQuery += newLine('RETURN id(n) AS id,' + entityType.dataProperties
                .filter(function (property) { return !property.isPartOfKey; })
                .map(function (property) {
                    return 'n.`' + property.nameOnServer + '` AS `' + property.name + '`';
                }).join(','));
            if (entityType.navigationProperties.length) {
                cypherQuery += ',';
                return cypherQuery += entityType.navigationProperties
                    .map(function (property, index) {
                        var navigationEntityType = property.entityType;
                        var navigationProperties = navigationEntityType.dataProperties;
                        return 'collect({ id: id(n' + index + '),' +
                            navigationProperties
                                .filter(function (property) { return !property.isPartOfKey; })
                                .map(function (navigationProperty) {
                                    return '`' + navigationProperty.name + '`: n' + index + '.`' + navigationProperty.nameOnServer + '`';
                                }).join(',')
                        + '}) AS ' + property.nameOnServer;
                    }).join(',');
            }
            return cypherQuery;
        },

        executeQuery: function (mappingContext) {
            var adapter = this;
            var deferred = Q.defer();
            var params = {
                type: 'POST',
                url: mappingContext.dataService.makeUrl('db/data/transaction/commit'),
                data: JSON.stringify({
                    statements: [
                        {
                            statement: this.buildCypherQuery(mappingContext),
                            resultDataContents: ['row']
                        }
                    ]
                }),
                dataType: 'json',
                contentType: 'application/json',
                success: function (httpResponse) {
                    var data = httpResponse.data;
                    deferred.resolve({ data: data, httpResponse: httpResponse });
                },
                error: function (httpResponse) {
                    throw new Error('HTTP error');
                }
            };
            if (mappingContext.dataService.useJsonp) {
                params.dataType = 'jsonp';
                params.crossDomain = true;
            }
            adapter.ajaxImpl.ajax(params);
            return deferred.promise;
        },

        buildCreateCypherQuery: function (saveContext, entity) {
            var cypherQuery = 'CYPHER 2.0';
            var helper = saveContext.entityManager.helper;
            var entityType = entity.entityType;
            var entityData = helper.unwrapInstance(entity);
            cypherQuery += newLine('CREATE (n:`' + entityType.shortName + '` {' +
                Object.keys(entityData)
                    .filter(function (propertyKey) { return !entityType.getDataProperty(propertyKey).isPartOfKey; })
                    .map(function (propertyKey) {
                        return '`' + propertyKey + '`:"' + entityData[propertyKey] + '"';
                    }).join(',')
            + '})');
            // Build unified response for write queries
            cypherQuery += newLine('RETURN [ { type: "' + entityType.name + '", id: id(n) } ] AS createdKeys, [{`tempValue`:' + entityData.id + ', `realValue`: id(n), `entityTypeName`: "' + entityType.name + '"}] AS keyMappings,' +
                '[{ id: id(n), `$type`: "' + entityType.name + '",' + entityType.dataProperties
                    .filter(function (property) { return !property.isPartOfKey; })
                    .map(function (property) {
                        return '`' + property.name + '`: n.`' + property.nameOnServer + '`';
                    }).join(',') +
            '}] AS createdEntities');
            return cypherQuery;
        },

        buildDeleteCypherQuery: function (saveContext, entity) {
            var cypherQuery = 'CYPHER 2.0';
            var helper = saveContext.entityManager.helper;
            cypherQuery += newLine('START n = node(' + entity.getProperty('id') + ')');
            cypherQuery += newLine('OPTIONAL MATCH n-[r]-()');
            cypherQuery += newLine('DELETE r, n');
            cypherQuery += newLine('RETURN [ { type: "' + entity.entityType.name + '", id: ' + entity.getProperty('id') + ' } ] AS deletedKeys');
            return cypherQuery;
        },

        buildModifyCypherQuery: function (saveContext, entity) {
            var cypherQuery = 'CYPHER 2.0';
            var helper = saveContext.entityManager.helper;
            cypherQuery += newLine('START n = node(' + entity.getProperty('id') + ')');
            cypherQuery += newLine('OPTIONAL MATCH n-[r]-()');
            cypherQuery += newLine('DELETE r, n');
            cypherQuery += newLine('RETURN [' + entity.getProperty('id') + '] AS deletedKeys');
            return cypherQuery;
        },

        buildCreateRelationshipsCypherQueries: function (saveContext, saveBundle, saveResult) {
            var cypherQueries = [];
            var keyMappings = saveResult.keyMappings;
            var mapKey = function (key) {
                for (var i = 0; i < keyMappings.length; i++) {
                    if (keyMappings[i].tempValue == key) { return keyMappings[i].realValue; }
                }
                return key;
            };
            saveBundle.entities.forEach(function (entity) {
                var key = entity.getProperty('id');
                var entityType = entity.entityType;
                entityType.dataProperties.forEach(function (property) {
                    if (typeof property.relatedNavigationProperty !== 'undefined') {
                        var relatedKey = entity.getProperty(property.name);
                        var relationType = property.relatedNavigationProperty.associationName;
                        var cypherQuery = 'CYPHER 2.0';
                        cypherQuery += newLine('START n = node(' + mapKey(key) + '), p = node(' + mapKey(relatedKey) + ')');
                        cypherQuery += newLine('CREATE UNIQUE n-[:`' + relationType + '`]->p');
                        cypherQueries.push(cypherQuery);
                    }
                });
            });
            return cypherQueries;
        },

        createTransactionStatements: function (saveContext, saveBundle) {
            var adapter = this;
            return saveBundle.entities.map(function (entity) {
                //var entityData = helper.unwrapChangedValues(entity, saveContext.entityManager.metadataStore);
                //var entityData = helper.unwrapInstance(entity);
                if (entity.entityAspect.entityState.isAdded()) {
                    return adapter.buildCreateCypherQuery(saveContext, entity);
                } else if (entity.entityAspect.entityState.isModified()) {
                    // ...
                } else if (entity.entityAspect.entityState.isDeleted()) {
                    return adapter.buildDeleteCypherQuery(saveContext, entity);
                }
            });
        },

        prepareSaveResult: function (saveContext, data, httpResponse) {
            var em = saveContext.entityManager;
            var entities = [];
            var mergedData = {
                createdKeys: [],
                deletedKeys: [],
                updatedKeys: [],
                keyMappings: [],
                createdEntities: []
            }, keys;
            data.forEach(function (statementData) {
                statementData.forEach(function (statementRow) {
                    Object.keys(statementRow).forEach(function (statementKey) {
                        mergedData[statementKey] = mergedData[statementKey] || [];
                        var statementValue = statementRow[statementKey];
                        if (statementValue && statementValue instanceof Array) {
                            mergedData[statementKey].push.apply(mergedData[statementKey], statementValue);
                        }
                    });
                });
            });
            keys = mergedData.createdKeys.concat(mergedData.deletedKeys, mergedData.updatedKeys);
            keys.forEach(function (key) {
                var entity = em.getEntityByKey(key.type, key.id);
                if (entity) {
                    entities.push(entity);
                }
            });
            if (mergedData.createdEntities.length > 0) {
                entities = entities.concat(mergedData.createdEntities);
            }
            return { entities: entities, keyMappings: mergedData.keyMappings, httpResponse: httpResponse };
        },

        saveChanges: function (saveContext, saveBundle) {
            var wrapStatements = function (statements) {
                return statements.map(function (statement) {
                    return { statement: statement, resultDataContents: ['row'] };
                });
            };
            var adapter = saveContext.adapter = this;
            var deferred = Q.defer();
            var baseParams = {
                type: 'POST', contentType: 'application/json', dataType: 'json'
            };
            adapter.ajaxImpl.ajax(breeze.core.extend(baseParams, {
                url: saveContext.dataService.makeUrl('db/data/transaction'),
                data: JSON.stringify({
                    statements: wrapStatements(adapter.createTransactionStatements(saveContext, saveBundle))
                }),
                success: function (httpResponse) {
                    var commitUrl = httpResponse.data.commit;
                    var transactionUrl = commitUrl.substring(0, commitUrl.lastIndexOf('/'));
                    var data = cypherResultsExtract(httpResponse.data);
                    var saveResult = adapter.prepareSaveResult(saveContext, data, httpResponse);
                    // Execute relationships creating query in current transaction
                    adapter.ajaxImpl.ajax(breeze.core.extend(baseParams, {
                        url: transactionUrl,
                        data: JSON.stringify({
                            statements: wrapStatements(adapter.buildCreateRelationshipsCypherQueries(saveContext, saveBundle, saveResult))
                        }),
                        success: function (httpResponse) {
                            saveResult.httpResponse = httpResponse;
                            adapter.ajaxImpl.ajax(breeze.core.extend(baseParams, {
                                type: 'DELETE',
                                url: transactionUrl,
                                success: function (httpResponse) {
                                    deferred.resolve(saveResult);
                                }
                            }));
                        },
                        error: function (httpResponse) { }
                    }));
                },
                error: function (httpResponse) {
                    throw new Error('HTTP error');
                }
            }));
            return deferred.promise;
        },

        jsonResultsAdapter: new JsonResultsAdapter({
            name: 'neo4j',
            extractResults: function (json) {
                var results = json.data.results[0];
                return results.data.map(function (values) {
                    var columns = results.columns;
                    var node = {};
                    columns.forEach(function (column, i) {
                        node[column] = values.row[i];
                    });
                    return node;
                });
            },
            visitNode: function (node, mappingContext, nodeContext) {
                return {
                    entityType: (function () {
                        if (typeof nodeContext.navigationProperty !== 'undefined') {
                            return nodeContext.navigationProperty.entityType;
                        } else {
                            return getEntityType(mappingContext, node.$type) || mappingContext.entityType ||
                                   node.$entityType || getEntityType(mappingContext)
                        }
                    })(),
                    nodeId: node.id
                };
            }
        })
    });


    breeze.config.registerAdapter('dataService', Neo4jDataService);
}, this));