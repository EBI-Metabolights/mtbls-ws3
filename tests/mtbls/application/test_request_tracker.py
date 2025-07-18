from mtbls.application.context.request_tracker import (
    RequestTracker,
    RequestTrackerModel,
    get_request_tracker,
)


class TestRequestTracker:
    def test_request_tracker(self):
        request_tracker = RequestTracker()

        assert request_tracker.client_var.get() == "-"
        assert request_tracker.request_id_var.get() == "-"
        assert request_tracker.user_id_var.get() == 0
        assert request_tracker.task_id_var.get() == "-"
        assert request_tracker.route_path_var.get() == "-"

    def test_get_request_tracker(self):
        tracker1 = get_request_tracker()
        tracker2 = get_request_tracker()
        assert tracker1 == tracker2

    def test_get_request_tracker_model_01(self):
        user_id = 123
        request_tracker = RequestTracker()
        request_tracker.client_var.set("client")
        request_tracker.request_id_var.set("request")
        request_tracker.user_id_var.set(user_id)
        request_tracker.task_id_var.set("task")
        request_tracker.route_path_var.set("route")
        assert request_tracker.client_var.get() == "client"
        assert request_tracker.request_id_var.get() == "request"
        assert request_tracker.user_id_var.get() == user_id
        assert request_tracker.task_id_var.get() == "task"
        assert request_tracker.route_path_var.get() == "route"

    def test_get_request_tracker_model_02(self):
        user_id = 123
        request_tracker = RequestTracker()
        request_tracker.client_var.set("client")
        request_tracker.request_id_var.set("request")
        request_tracker.user_id_var.set(user_id)
        request_tracker.task_id_var.set("task")
        request_tracker.route_path_var.set("route")
        model = RequestTrackerModel()
        request_tracker.update_request_tracker(model)
        model = request_tracker.get_request_tracker_model()
        assert request_tracker.client_var.get() == "-"
        assert request_tracker.request_id_var.get() == "-"
        assert request_tracker.user_id_var.get() == 0
        assert request_tracker.task_id_var.get() == "-"
        assert request_tracker.route_path_var.get() == "-"
