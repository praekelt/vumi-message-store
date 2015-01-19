"""
Interfaces for various parts of the message store.

The idea is that there will potentially be multiple implementations, and some
thing may implement multiple interfaces.
"""

from zope.interface import Interface


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
