(function (factory) {
    if (breeze) {
        factory(breeze);
    } else if (typeof require === "function" && typeof exports === "object" && typeof module === "object") {
        // CommonJS or Node: hard-coded dependency on "breeze"
        factory(require("breeze"));
    } else if (typeof define === "function" && define["amd"] && !breeze) {
        // AMD anonymous module with hard-coded dependency on "breeze"
        define(["breeze"], factory);
    }
}(function (breeze) {
    "use strict";
    var core = breeze.core;

    var MetadataStore = breeze.MetadataStore;
    var JsonResultsAdapter = breeze.JsonResultsAdapter;
    var DataProperty = breeze.DataProperty;

    var OData;

    var ctor = function () {
        this.name = "OData";
    };

    var fn = ctor.prototype; // minifies better (as seen in jQuery)

    fn.initialize = function () {
        OData = core.requireLib("OData", "Needed to support remote OData services");
        OData.jsonHandler.recognizeDates = true;
    };


    fn.executeQuery = function (mappingContext) {

        var deferred = Q.defer();
        var url = mappingContext.getUrl();

        var paramSeparation = '?';
        if (url.indexOf('?') > -1)
            paramSeparation = '&';
        if (mappingContext.query && mappingContext.query.parameters) {
            var queryOptions = mappingContext.query.parameters;
            for (var qoName in queryOptions) {
                var qoValue = queryOptions[qoName];
                if (qoValue !== undefined) {
                    if (qoValue instanceof Array) {
                        qoValue.forEach(function (qov) {
                            url += paramSeparation + (qoName + "=" + encodeURIComponent(qov));
                        });
                    } else {
                        url += paramSeparation + (qoName + "=" + encodeURIComponent(qoValue));
                    }
                }
                paramSeparation = '&';
            }
        }

        var serviceUrl = mappingContext.entityManager.serviceName;
        var methodUrl = url.replace(serviceUrl, '');

        OData.request({
            requestUri: serviceUrl + "/$batch",
            method: "POST",
            data: {
                __batchRequests: [
                   { requestUri: methodUrl, method: "GET" }
                ]
            }
        }, function (batchData, response) {
            if (batchData) {
                var response = batchData.__batchResponses[0];
                var data = response.data;
                if (response.statusCode == 200) {
                    var inlineCount;
                    if (data.__count) {
                        // OData can return data.__count as a string
                        inlineCount = parseInt(data.__count, 10);
                    }
                    return deferred.resolve({ results: data.results, inlineCount: inlineCount });
                }
            }
            return deferred.reject(createError({ response: response }, mappingContext.url));
        }, function (error) {
            return deferred.reject(createError(error, mappingContext.url));
        }, OData.batchHandler);
        return deferred.promise;
    };


    fn.fetchMetadata = function (metadataStore, dataService) {

        var deferred = Q.defer();

        var serviceName = dataService.serviceName;
        var url = dataService.makeUrl('$metadata');

        if (dataService.customMetadataUrl) {
            url = dataService.customMetadataUrl;
        }

        //OData.read({
        //    requestUri: url,
        //    headers: {
        //        "Accept": "application/json",
        //    }
        //},
        OData.read(url,
            function (data, response) {
                var statusCode = response.statusCode;
                if ((!statusCode) || statusCode == 203 || statusCode >= 400) {
                    return deferred.reject(createError({ response: response }, url));
                }
                // data.dataServices.schema is an array of schemas. with properties of 
                // entityContainer[], association[], entityType[], and namespace.
                if (!data || !data.dataServices) {
                    try {
                        data = JSON.parse(response.body);
                        if (data.schema) {
                            data = {
                                "version": "1.0",
                                "dataServices":
                                    {
                                        "dataServiceVersion": "1.0",
                                        "maxDataServiceVersion": "3.0",
                                        "schema": [data.schema]
                                    }
                            };
                        }
                    }
                    finally {
                        if (!data || !data.dataServices) {
                            var error = new Error("Metadata query failed for: " + url);
                            return deferred.reject(error);
                        }
                    }
                }
                var csdlMetadata = data.dataServices;

                // might have been fetched by another query
                if (!metadataStore.hasMetadataFor(serviceName)) {
                    try {
                        metadataStore.importMetadata(csdlMetadata);
                    } catch (e) {
                        return deferred.reject(new Error("Metadata query failed for " + url + "; Unable to process returned metadata: " + e.message));
                    }

                    metadataStore.addDataService(dataService);
                }

                return deferred.resolve(csdlMetadata);

            }, function (error) {
                var err = createError(error, url);
                err.message = "Metadata query failed for: " + url + "; " + (err.message || "");
                return deferred.reject(err);
            },
            OData.metadataHandler
        );

        return deferred.promise;

    };

    fn.getRoutePrefix = function (dataService) { return ''; /* see webApiODataCtor */ }

    fn.saveChanges = function (saveContext, saveBundle) {

        var deferred = Q.defer();

        var helper = saveContext.entityManager.helper;
        var url = saveContext.dataService.makeUrl("$batch");

        var requestData = createChangeRequests(saveContext, saveBundle);
        var innerEntities = requestData.__innerEntities || [];
        delete requestData.__innerEntities;

        if (requestData.__batchRequests[0].__changeRequests.length == 0) {
            deferred.resolve({ entities: innerEntities, keyMappings: [] });
            return deferred.promise;
        }

        var routePrefix = this.getRoutePrefix(saveContext.dataService);
        var requestData = createChangeRequests(saveContext, saveBundle, routePrefix);
        var tempKeys = saveContext.tempKeys;
        var contentKeys = saveContext.contentKeys;
        var that = this;
        OData.request({
            headers: { "DataServiceVersion": "2.0" },
            requestUri: url,
            method: "POST",
            data: requestData
        }, function (data, response) {
            var statusCode = response.statusCode;
            if ((!statusCode) || statusCode == 203 || statusCode >= 400) {
                return deferred.reject(createError({ response: response }, url));
            }
            var entities = innerEntities;
            var keyMappings = [];
            var saveResult = { entities: entities, keyMappings: keyMappings };
            data.__batchResponses.forEach(function (br) {
                br.__changeResponses.forEach(function (cr) {
                    var response = cr.response || cr;
                    var statusCode = response.statusCode;
                    if ((!statusCode) || statusCode >= 400) {
                        return deferred.reject(createError(cr, url));
                    }

                    var contentId = cr.headers["Content-ID"];

                    var rawEntity = cr.data;
                    if (rawEntity) {
                        var tempKey = tempKeys[contentId];
                        if (tempKey) {
                            var entityType = tempKey.entityType;
                            if (entityType.autoGeneratedKeyType !== breeze.AutoGeneratedKeyType.None) {
                                var tempValue = tempKey.values[0];
                                var realKey = entityType.getEntityKeyFromRawEntity(rawEntity, DataProperty.getRawValueFromServer);
                                var keyMapping = { entityTypeName: entityType.name, tempValue: tempValue, realValue: realKey.values[0] };
                                keyMappings.push(keyMapping);
                            }
                        }
                        entities.push(rawEntity);
                    } else {
                        var origEntity = contentKeys[contentId];
                        if (origEntity) //
                            entities.push(origEntity);
                    }
                });
            });
            return deferred.resolve(saveResult);
        }, function (err) {
            return deferred.reject(createError(err, url));
        }, OData.batchHandler);

        return deferred.promise;

    };

    fn.jsonResultsAdapter = new JsonResultsAdapter({
        name: "OData_default",

        visitNode: function (node, mappingContext, nodeContext) {
            var result = {};
            if (node == null) return result;
            var metadata = node.__metadata;
            if (metadata != null) {
                // TODO: may be able to make this more efficient by caching of the previous value.
                var entityTypeName = MetadataStore.normalizeTypeName(metadata.type);
                var et = entityTypeName && mappingContext.entityManager.metadataStore.getEntityType(entityTypeName, true);
                // if (et && et._mappedPropertiesCount === Object.keys(node).length - 1) {
                if (et && et._mappedPropertiesCount <= Object.keys(node).length - 1) {
                    result.entityType = et;
                    var baseUri = mappingContext.dataService.serviceName;
                    var uriKey = metadata.uri || metadata.id;
                    if (core.stringStartsWith(uriKey, baseUri)) {
                        uriKey = uriKey.substring(baseUri.length);
                    }
                    result.extraMetadata = {
                        uriKey: uriKey,
                        etag: metadata.etag
                    }
                }
            }
            // OData v3 - projection arrays will be enclosed in a results array
            if (node.results) {
                result.node = node.results;
            }

            var propertyName = nodeContext.propertyName;
            result.ignore = node.__deferred != null || propertyName === "__metadata" ||
                // EntityKey properties can be produced by EDMX models
                (propertyName === "EntityKey" && node.$type && core.stringStartsWith(node.$type, "System.Data"));
            return result;
        }

    });

    function transformValue(prop, val) {
        if (prop.isUnmapped) return undefined;
        if (prop.dataType === breeze.DataType.DateTimeOffset) {
            // The datajs lib tries to treat client dateTimes that are defined as DateTimeOffset on the server differently
            // from other dateTimes. This fix compensates before the save.
            val = val && new Date(val.getTime() - (val.getTimezoneOffset() * 60000));
        } else if (prop.dataType.quoteJsonOData) {
            val = val != null ? val.toString() : val;
        }
        return val;
    }

    function _getEntityId(entity) {
        var prefix = "", sufix = "";
        if (entity.entityType.keyProperties[0].dataType.name == "Guid") {
            prefix = "guid'";
            sufix = "'";
        }
        return prefix + entity.entityAspect.getKey().values[0] + sufix;
    }

    function _getEntityUri(prefix, entity) {
        var extraMetadata = entity.entityAspect.extraMetadata;
        return extraMetadata != null ? (extraMetadata.uri || extraMetadata.id)
            : (location.origin + prefix + entity.entityType.defaultResourceName + "(" + _getEntityId(entity) + ")");
    }

    function createChangeRequests(saveContext, saveBundle, routePrefix) {
        var innerEntities = [];
        var readedAssociations = [];
        var createdCREntities = [];
        var linksRequest = [];
        var changeRequests = [];
        var tempKeys = [];
        var contentKeys = [];
        var baseUri = saveContext.dataService.serviceName;
        var entityManager = saveContext.entityManager;
        var helper = entityManager.helper;
        var id = 0;
        saveBundle.entities.forEach(function (entity) {
            createdCREntities.push(entity);
            var aspect = entity.entityAspect;
            id = id + 1; // we are deliberately skipping id=0 because Content-ID = 0 seems to be ignored.
            var request = { headers: { "Content-ID": id, "DataServiceVersion": "2.0" } };
            contentKeys[id] = entity;

            if (entity.entityAspect.entityState.isModified()) {
                aspect.inseredLinks.forEach(function (inseredLink) {
                    if (!inseredLink.entity.entityAspect.entityState.isAdded()
                        && !inseredLink.entity.entityAspect.entityState.isDeleted()) {
                        var linkRequest = { headers: { "Content-ID": id, "DataServiceVersion": "3.0" } };
                        linkRequest.requestUri = _getEntityUri(baseUri, entity) + "/$links/" + inseredLink.np.name;
                        linkRequest.method = "POST";

                        var baseType = inseredLink.entity.entityType;
                        while (baseType.baseEntityType)
                            baseType = baseType.baseEntityType;

                        linkRequest.data = {
                            uri: (inseredLink.entity.entityAspect.extraMetadata ? inseredLink.entity.entityAspect.extraMetadata.id : null) ||
                                prefix + baseType.defaultResourceName + "(" + _getEntityId(inseredLink.entity) + ")"
                        };

                        linksRequest.push(linkRequest);
                    }
                });

                aspect.removedLinks.forEach(function (removedLink) {
                    if (createdCREntities.indexOf(removedLink.entity) == -1) {
                        if (!removedLink.entity.entityAspect.entityState.isAdded()
                            && !removedLink.entity.entityAspect.entityState.isDeleted()) {
                            var linkRequest = { headers: { "Content-ID": id, "DataServiceVersion": "3.0" } };
                            // DELETE /OData/OData.svc/Categories(1)/$links/Products(10)
                            linkRequest.requestUri = _getEntityUri(baseUri, aspect.entity)
                                + "/$links/" + removedLink.np.name + "(" + _getEntityId(removedLink.entity) + ")";
                            linkRequest.method = "DELETE";
                            linksRequest.push(linkRequest);
                        }
                    }
                });
            }

            if (aspect.entityState.isAdded()) {
                var options = { readedAssociations: readedAssociations };
                insertRequest(request, entity.entityType, routePrefix);
                request.method = "POST";
                request.data = helper.unwrapInstance(entity, transformValue, options);
                tempKeys[id] = aspect.getKey();
                // should be a PATCH/MERGE
                if (options.isIgnored || Object.keys(request.data).length == 0) {
                    innerEntities.push(entity);
                    id--;
                    return;
                }
            } else if (aspect.entityState.isModified()) {
                updateDeleteMergeRequest(request, aspect, baseUri, routePrefix);
                request.method = "MERGE";
                request.data = helper.unwrapChangedValues(entity, entityManager.metadataStore, transformValue);
                // should be a PATCH/MERGE
                if (Object.keys(request.data).length == 0) {
                    innerEntities.push(entity);
                    id--;
                    return;
                }
            } else if (aspect.entityState.isDeleted()) {
                updateDeleteMergeRequest(request, aspect, baseUri, routePrefix);
                request.method = "DELETE";
            } else {
                return;
            }
            changeRequests.push(request);
        });

        linksRequest.forEach(function (link) {
            link.headers["Content-ID"] = ++id;
            changeRequests.push(link);
        });

        saveContext.contentKeys = contentKeys;
        saveContext.tempKeys = tempKeys;
        return {
            __innerEntities: innerEntities,
            __batchRequests: [{
                __changeRequests: changeRequests
            }]
        };

    }

    function insertRequest(request, entityType, routePrefix) {
        var lastBaseType = null;
        var sbaseType = entityType;
        var defaultResourceName = entityType.defaultResourceName;
        do {
            lastBaseType = sbaseType;
            sbaseType = sbaseType.baseEntityType;
        } while (sbaseType);

        if (lastBaseType !== entityType) {
            var namespace = entityType.namespace;
            var className = entityType.name.replace(":#" + namespace, "");
            defaultResourceName = lastBaseType.defaultResourceName + "/" + namespace + "." + className;
        }

        routePrefix = routePrefix || '';
        request.requestUri = routePrefix + defaultResourceName;
    }

    function updateDeleteMergeRequest(request, aspect, baseUri, routePrefix) {
        var extraMetadata = aspect.extraMetadata;
        var uri = extraMetadata.uri || extraMetadata.id;
        if (core.stringStartsWith(uri, baseUri)) {
            uri = routePrefix + uri.substring(baseUri.length);
        }
        request.requestUri = uri;
        if (aspect.extraMetadata &&
            aspect.extraMetadata.etag) {
            request.headers["If-Match"] = extraMetadata.etag;
        }
    }

    function createError(error, url) {
        // OData errors can have the message buried very deeply - and nonobviously
        // this code is tricky so be careful changing the response.body parsing.
        var result = new Error();
        var response = error && error.response;
        if (!response) {
            // in case DataJS returns "No handler for this data"
            result.message = error;
            result.statusText = error;
            return result;
        }
        result.message = response.statusText;
        result.statusText = response.statusText;
        result.status = response.statusCode;
        // non std
        if (url) result.url = url;
        result.body = response.body;
        if (response.body) {
            var nextErr;
            try {
                var body = JSON.parse(response.body);
                result.body = body;
                // OData v3 logic
                if (body['odata.error']) {
                    body = body['odata.error'];
                }
                var msg = "";
                do {
                    nextErr = body.error || body.innererror;
                    if (!nextErr) msg = msg + getMessage(body);
                    nextErr = nextErr || body.internalexception;
                    body = nextErr || body;
                } while (nextErr);
                if (msg.length > 0) {
                    result.message = msg;
                }
            } catch (e) {

            }
        }
        return result;
    }

    function getMessage(body) {
        var msg = body.message || "";
        return ((typeof (msg) === "string") ? msg : msg.value) + "; ";
    }

    breeze.config.registerAdapter("dataService", ctor);


    var webApiODataCtor = function () {
        this.name = "webApiOData";
    }

    breeze.core.extend(webApiODataCtor.prototype, fn);

    webApiODataCtor.prototype.getRoutePrefix = function (dataService) {
        // Get the routePrefix from a Web API OData service name.
        // Web API OData requires inclusion of the routePrefix in the Uri of a batch subrequest
        // By convention, Breeze developers add the Web API OData routePrefix to the end of the serviceName
        // e.g. the routePrefix in 'http://localhost:55802/odata/' is 'odata/'
        var segments = dataService.serviceName.split('/');
        var last = segments.length - 1;
        var routePrefix = segments[last] || segments[last - 1];
        routePrefix = routePrefix ? routePrefix += '/' : '';
        return routePrefix;
    };

    breeze.config.registerAdapter("dataService", webApiODataCtor);

}));