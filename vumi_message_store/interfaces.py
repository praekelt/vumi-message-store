"""
Interfaces for various parts of the message store.

The idea is that there will potentially be multiple implementations, and some
thing may implement multiple interfaces.
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

        TODO: document params.
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

    def add_event(event):
        """
        Add an event to the message store.

        :param event:
            The TransportEvent to add.

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
