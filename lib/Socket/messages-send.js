"use strict";

var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};

Object.defineProperty(exports, "__esModule", { value: true });
exports.makeMessagesSocket = void 0;

const boom_1 = require("@hapi/boom");
const node_cache_1 = __importDefault(require("node-cache"));
const WAProto_1 = require("../../WAProto");
const Defaults_1 = require("../Defaults");
const axios_1 = require("axios");
const Types_1 = require("../Types");
const Utils_1 = require("../Utils");
const link_preview_1 = require("../Utils/link-preview");
const WABinary_1 = require("../WABinary");
const newsletter_1 = require("./newsletter");
const WAUSync_1 = require("../WAUSync");
const kikyy = require("./dugong");

var ListType = WAProto_1.proto.Message.ListMessage.ListType;

const makeMessagesSocket = (config) => {

    const {
        logger,
        linkPreviewImageThumbnailWidth,
        generateHighQualityLinkPreview,
        options: axiosOptions,
        patchMessageBeforeSending = async(m) => m
    } = config;

    const sock = (0, newsletter_1.makeNewsletterSocket)(config);

    const {
        ev,
        authState,
        processingMutex,
        signalRepository,
        upsertMessage,
        query,
        fetchPrivacySettings,
        generateMessageTag,
        sendNode,
        groupMetadata,
        groupToggleEphemeral,
        executeUSyncQuery
    } = sock;

    const userDevicesCache =
        config.userDevicesCache ||
        new node_cache_1.default({
            stdTTL: Defaults_1.DEFAULT_CACHE_TTLS.USER_DEVICES,
            useClones: false
        });

    let mediaConn;

    const refreshMediaConn = async(forceGet = false) => {
        const media = await mediaConn;

        if (
            !media ||
            forceGet ||
            (new Date().getTime() - media.fetchDate.getTime()) >
            media.ttl * 1000
        ) {
            mediaConn = (async() => {
                const result = await query({
                    tag: "iq",
                    attrs: {
                        type: "set",
                        xmlns: "w:m",
                        to: WABinary_1.S_WHATSAPP_NET
                    },
                    content: [{
                        tag: "media_conn",
                        attrs: {}
                    }]
                });

                const mediaConnNode =
                    WABinary_1.getBinaryNodeChild(result, "media_conn");

                const node = {
                    hosts: WABinary_1.getBinaryNodeChildren(
                        mediaConnNode,
                        "host"
                    ).map(({ attrs }) => ({
                        hostname: attrs.hostname,
                        maxContentLengthBytes: +attrs.maxContentLengthBytes
                    })),
                    auth: mediaConnNode.attrs.auth,
                    ttl: +mediaConnNode.attrs.ttl,
                    fetchDate: new Date()
                };

                logger.debug("fetched media conn");

                return node;
            })();
        }

        return mediaConn;
    };

    const sendReceipt = async(jid, participant, messageIds, type) => {

        const node = {
            tag: "receipt",
            attrs: {
                id: messageIds[0]
            }
        };

        const isReadReceipt =
            type === "read" || type === "read-self";

        if (isReadReceipt) {
            node.attrs.t =
                (0, Utils_1.unixTimestampSeconds)().toString();
        }

        if (type === "sender" && WABinary_1.isJidUser(jid)) {
            node.attrs.recipient = jid;
            node.attrs.to = participant;
        } else {
            node.attrs.to = jid;

            if (participant) {
                node.attrs.participant = participant;
            }
        }

        if (type) {
            node.attrs.type =
                WABinary_1.isJidNewsLetter(jid)
                ? "read-self"
                : type;
        }

        const remainingMessageIds = messageIds.slice(1);

        if (remainingMessageIds.length) {
            node.content = [{
                tag: "list",
                attrs: {},
                content: remainingMessageIds.map((id) => ({
                    tag: "item",
                    attrs: { id }
                }))
            }];
        }

        logger.debug({
                attrs: node.attrs,
                messageIds
            },
            "sending receipt for messages"
        );

        await sendNode(node);
    };

    const sendReceipts = async(keys, type) => {
        const recps =
            (0, Utils_1.aggregateMessageKeysNotFromMe)(keys);

        for (const {
                jid,
                participant,
                messageIds
            } of recps) {
            await sendReceipt(
                jid,
                participant,
                messageIds,
                type
            );
        }
    };

    const readMessages = async(keys) => {
        const privacySettings =
            await fetchPrivacySettings();

        const readType =
            privacySettings.readreceipts === "all"
            ?
            "read" :
            "read-self";

        await sendReceipts(keys, readType);
    };

    const getUSyncDevices = async(
        jids,
        useCache,
        ignoreZeroDevices
    ) => {

        const deviceResults = [];

        if (!useCache) {
            logger.debug("not using cache for devices");
        }

        const toFetch = [];

        jids = Array.from(new Set(jids));

        for (let jid of jids) {

            const user =
                WABinary_1.jidDecode(jid)?.user;

            jid = WABinary_1.jidNormalizedUser(jid);

            if (useCache) {

                const devices =
                    userDevicesCache.get(user);

                if (devices) {
                    deviceResults.push(...devices);

                    logger.trace({
                            user
                        },
                        "using cache for devices"
                    );

                } else {
                    toFetch.push(jid);
                }

            } else {
                toFetch.push(jid);
            }
        }

        if (!toFetch.length) {
            return deviceResults;
        }

        const usyncQuery =
            new WAUSync_1.USyncQuery()
            .withContext("message")
            .withDeviceProtocol();

        for (const jid of toFetch) {
            usyncQuery.withUser(
                new WAUSync_1.USyncUser().withId(jid)
            );
        }

        const result =
            await executeUSyncQuery(usyncQuery);

        if (result) {

            const extracted =
                Utils_1.extractDeviceJids(
                    result?.list,
                    authState.creds.me.id,
                    ignoreZeroDevices
                );

            const deviceMap = {};

            for (const item of extracted) {

                deviceMap[item.user] =
                    deviceMap[item.user] || [];

                deviceMap[item.user].push(item);

                deviceResults.push(item);
            }

            for (const key in deviceMap) {
                userDevicesCache.set(
                    key,
                    deviceMap[key]
                );
            }
        }

        return deviceResults;
    };

    const assertSessions = async(jids, force) => {

        let didFetchNewSession = false;
        let jidsRequiringFetch = [];

        if (force) {

            jidsRequiringFetch = jids;

        } else {

            const addrs = jids.map((jid) =>
                signalRepository.jidToSignalProtocolAddress(jid)
            );

            const sessions =
                await authState.keys.get(
                    "session",
                    addrs
                );

            for (const jid of jids) {

                const signalId =
                    signalRepository.jidToSignalProtocolAddress(jid);

                if (!sessions[signalId]) {
                    jidsRequiringFetch.push(jid);
                }
            }
        }

        if (jidsRequiringFetch.length) {

            logger.debug({
                    jidsRequiringFetch
                },
                "fetching sessions"
            );

            const result = await query({
                tag: "iq",
                attrs: {
                    xmlns: "encrypt",
                    type: "get",
                    to: WABinary_1.S_WHATSAPP_NET
                },
                content: [{
                    tag: "key",
                    attrs: {},
                    content: jidsRequiringFetch.map(
                        (jid) => ({
                            tag: "user",
                            attrs: { jid }
                        })
                    )
                }]
            });

            await (0,
                Utils_1.parseAndInjectE2ESessions)(
                result,
                signalRepository
            );

            didFetchNewSession = true;
        }

        return didFetchNewSession;
    };

    const waUploadToServer =
        (0, Utils_1.getWAUploadToServer)(
            config,
            refreshMediaConn
        );

    const rahmi = new kikyy(
        Utils_1,
        waUploadToServer
    );

    return {
        ...sock,
        sendReceipt,
        sendReceipts,
        readMessages,
        getUSyncDevices,
        assertSessions,
        refreshMediaConn,
        waUploadToServer,
        rahmi
    };
};

exports.makeMessagesSocket = makeMessagesSocket;
