"""
Interfaces for various parts of the message store.

The idea is that there will potentially be multiple implementations, and some
things may implement multiple interfaces.
"""

from zope.interface import Interface


class IMessageStoreBatchManager(Interface):
    """
    Interface for a message store batch manager.

    This is for managing tags and batches.
    """

    def batch_start(tags=(), **metadata):
        """
        Create a new message batch.

        :param tags:
            Sequence of tags to add to the new batch.
        :param **metadata:
            Keyword parameters containing batch metadata.

        :returns:
            The batch identifier for the new batch.
            If async, a Deferred is returned instead.
        """

    def batch_done(batch_id):
        """
        Clear all references to a batch from its tags.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :returns:
            ``None``.
            If async, a Deferred is returned instead.
        """

    def get_batch(batch_id):
        """
        Get a batch from the message store.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :returns:
            A Batch model object.
            If async, a Deferred is returned instead.
        """

    def get_tag_info(tag):
        """
        Get tag information from the message store.

        :param tag:
            A tag identifier, either in tuple format or a flattened string.

        :returns:
            A CurrentTag model object.
            If async, a Deferred is returned instead.
        """

    def rebuild_cache(batch_id, qms):
        """
        Rebuild the cache using the provided IQueryMessageStore implementation.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :param qms:
            An `IQueryMessageStore` provider to rebuild the cache from.

        :returns:
            ``None``.
            If async, a Deferred is returned instead.
        """


class IOperationalMessageStore(Interface):
    """
    Interface for an operational message store.

    This is for reading and writing messages during their transit through the
    system where a very limited feature set is required, but where latency
    matters.
    """

    def add_inbound_message(msg, batch_ids=()):
        """
        Add an inbound mesage to the message store.

        :param msg:
            The TransportUserMessage to add.
        :param batch_ids:
            Sequence of batch identifiers to add the message to.

        :returns:
            ``None``.
            If async, a Deferred is returned instead.
        """

    def get_inbound_message(msg_id):
        """
        Get an inbound mesage from the message store.

        :param msg_id:
            The identifier of the message to retrieve.

        :returns:
            A TransportUserMessage, or ``None`` if the message is not found.
            If async, a Deferred is returned instead.
        """

    def add_outbound_message(msg, batch_ids=()):
        """
        Add an outbound mesage to the message store.

        :param msg:
            The TransportUserMessage to add.
        :param batch_ids:
            Sequence of batch identifiers to add the message to.

        :returns:
            ``None``.
            If async, a Deferred is returned instead.
        """

    def get_outbound_message(msg_id):
        """
        Get an outbound mesage from the message store.

        :param msg_id:
            The identifier of the message to retrieve.

        :returns:
            A TransportUserMessage, or ``None`` if the message is not found.
            If async, a Deferred is returned instead.
        """

    def add_event(event, batch_ids=()):
        """
        Add an event to the message store.

        :param event:
            The TransportEvent to add.
        :param batch_ids:
            Sequence of batch identifiers to add the event to.

        :returns:
            ``None``.
            If async, a Deferred is returned instead.
        """

    def get_event(event_id):
        """
        Get an event from the message store.

        :param event_id:
            The identifier of the event to retrieve.

        :returns:
            A TransportEvent, or ``None`` if the event is not found.
            If async, a Deferred is returned instead.
        """

    def get_tag_info(tag):
        """
        Get tag information from the message store.

        :param tag:
            A tag identifier, either in tuple format or a flattened string.

        :returns:
            A CurrentTag model object.
            If async, a Deferred is returned instead.
        """


class IQueryMessageStore(Interface):
    """
    Interface for a query message store.

    This is for querying stored messages. All operations are read-only.
    """

    def get_inbound_message(msg_id):
        """
        Get an inbound mesage from the message store.

        :param msg_id:
            The identifier of the message to retrieve.

        :returns:
            A TransportUserMessage, or ``None`` if the message is not found.
            If async, a Deferred is returned instead.
        """

    def get_outbound_message(msg_id):
        """
        Get an outbound mesage from the message store.

        :param msg_id:
            The identifier of the message to retrieve.

        :returns:
            A TransportUserMessage, or ``None`` if the message is not found.
            If async, a Deferred is returned instead.
        """

    def get_event(event_id):
        """
        Get an event from the message store.

        :param event_id:
            The identifier of the event to retrieve.

        :returns:
            A TransportEvent, or ``None`` if the event is not found.
            If async, a Deferred is returned instead.
        """

    def list_batch_inbound_keys(batch_id, max_results=None, continuation=None):
        """
        List inbound message keys for the given batch.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :returns:
            An IndexPage object containing a list of inbound message keys.
            If async, a Deferred is returned instead.
        """

    def list_batch_outbound_keys(batch_id, max_results=None,
                                 continuation=None):
        """
        List outbound message keys for the given batch.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :returns:
            An IndexPage object containing a list of outbound message keys.
            If async, a Deferred is returned instead.
        """

    def list_message_event_keys(message_id, max_results=None,
                                continuation=None):
        """
        List event keys for the given outbound message.

        :param message_id:
            The message identifier to find events for.

        :returns:
            An IndexPage object containing a list of event keys.
            If async, a Deferred is returned instead.
        """

    def list_batch_inbound_keys_with_timestamps(batch_id, start=None,
                                                end=None):
        """
        List inbound message keys with timestamps for the given batch.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :param start:
            Timestamp denoting the start of a range query.

        :param end:
            Timestamp denoting the end of a range query.

        :returns:
            An IndexPage object containing a list of tuples of inbound message
            key and timestamp.
            If async, a Deferred is returned instead.
        """

    def list_batch_outbound_keys_with_timestamps(batch_id, start=None,
                                                 end=None):
        """
        List outbound message keys with timestamps for the given batch.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :param start:
            Timestamp denoting the start of a range query.

        :param end:
            Timestamp denoting the end of a range query.

        :returns:
            An IndexPage object containing a list of tuples of outbound message
            key and timestamp.
            If async, a Deferred is returned instead.
        """

    def list_batch_inbound_keys_with_addresses(batch_id, start=None, end=None):
        """
        List inbound message keys with timestamps and source addresses for the
        given batch.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :param start:
            Timestamp denoting the start of a range query.

        :param end:
            Timestamp denoting the end of a range query.

        :returns:
            An IndexPage object containing a list of tuples of inbound message
            key, timestamp, and from_addr.
            If async, a Deferred is returned instead.
        """

    def list_batch_outbound_keys_with_addresses(batch_id, start=None,
                                                end=None):
        """
        List outbound message keys with timestamps and destination addresses
        for the given batch.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :param start:
            Timestamp denoting the start of a range query.

        :param end:
            Timestamp denoting the end of a range query.

        :returns:
            An IndexPage object containing a list of tuples of outbound message
            key, timestamp, and to_addr.
            If async, a Deferred is returned instead.
        """

    def list_message_event_keys_with_statuses(message_id):
        """
        List event keys with timestamps and statuses for the given outbound
        message.

        :param message_id:
            The message identifier to find events for.

        :returns:
            An IndexPage object containing a list tupled of event key,
            timestamp, and event status.
            If async, a Deferred is returned instead.
        """

    def list_batch_events(batch_id, start=None, end=None):
        """
        List event keys with timestamps and statuses for the given batch.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :param start:
            Timestamp denoting the start of a range query.

        :param end:
            Timestamp denoting the end of a range query.

        :returns:
            An IndexPage object containing a list of tuples of event key,
            timestamp, and statuses.
            If async, a Deferred is returned instead.
        """

    def get_batch_info_status(batch_id):
        """
        Return a dictionary containing the latest event stats for the given
        batch_id.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :returns:
            A dictionary containing counts for sent messages and events types.
            If async, a Deferred is returned instead.
        """

    def list_batch_recent_inbound_keys(batch_id, with_timestamp=False):
        """
        Return the list of recent inbound message keys in descending order by
        timestamp.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :param bool with_timestamp:
            If set to ``True``, timestamps will be included in the result.

        :returns:
            A list of message keys (the default) or (key, timestamp) tuples (if
            ``with_timestamp`` is set to ``True``).
            If async, a Deferred is returned instead.
        """

    def list_batch_recent_outbound_keys(batch_id, with_timestamp=False):
        """
        Return the list of recent outbound message keys in descending order by
        timestamp.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :param bool with_timestamp:
            If set to ``True``, timestamps will be included in the result.

        :returns:
            A list of message keys (the default) or (key, timestamp) tuples (if
            ``with_timestamp`` is set to ``True``).
            If async, a Deferred is returned instead.
        """

    def get_batch_inbound_count(batch_id):
        """
        Return the count of inbound messages.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :returns:
            The number of inbound messages in the batch.
            If async, a Deferred is returned instead.
        """

    def get_batch_outbound_count(batch_id):
        """
        Return the count of outbound messages.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :returns:
            The number of outbound messages in the batch.
            If async, a Deferred is returned instead.
        """

    def get_batch_event_count(batch_id):
        """
        Return the count of events.

        :param batch_id:
            The batch identifier for the batch to operate on.

        :returns:
            The number of events in the batch.
            If async, a Deferred is returned instead.
        """
