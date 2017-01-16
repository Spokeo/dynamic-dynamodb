class TableMetricBuffer:
    def __init__(self, sampling_window_in_sec = 60):
        self.prev_throttled_reads_cnt = 0
        self.curr_throttled_reads_cnt = 0
        self.prev_provision_reads_cnt = 0
        self.curr_provision_reads_cnt = 0
        self.prev_consuming_reads_cnt = 0
        self.curr_consuming_reads_cnt = 0

        self.prev_throttled_write_cnt = 0
        self.curr_throttled_write_cnt = 0
        self.prev_provision_write_cnt = 0
        self.curr_provision_write_cnt = 0
        self.prev_consuming_write_cnt = 0
        self.curr_consuming_write_cnt = 0

        self.sampling_window_in_sec = sampling_window_in_sec

    def check_and_calc_weighted_units_when_throttle(self, curr_throttled_cnt, prev_throttled_cnt,
                                                          curr_consuming_cnt, prev_consuming_cnt,
                                                          curr_provision_cnt, updated_units):
        if (0 < curr_throttled_cnt):
            throttle_events_delta = curr_throttled_cnt - prev_throttled_cnt
            throttle_events_delta_to_total_req_ratio = (
                float(throttle_events_delta) / float(prev_throttled_cnt + prev_consuming_cnt))
            current_units = curr_provision_cnt / self.sampling_window_in_sec
            target_units = int(current_units * (throttle_events_delta_to_total_req_ratio + 1.0))
            if (target_units > updated_units): updated_units = target_units

        return updated_units

    def check_and_calculate_weighted_read_units(self, updated_read_units):
        return check_and_calc_weighted_units_when_throttle(
            self.curr_throttled_reads_cnt, self.prev_throttled_reads_cnt,
            self.curr_consuming_reads_cnt, self.prev_consuming_reads_cnt,
            self.curr_provision_reads_cnt, updated_read_units)

    def check_and_calculate_weighted_write_units(self, updated_write_units):
        return check_and_calc_weighted_units_when_throttle(
            self.curr_throttled_write_cnt, self.prev_throttled_write_cnt,
            self.curr_consuming_write_cnt, self.prev_consuming_write_cnt,
            self.curr_provision_write_cnt, updated_write_units)

    def to_dict(self):
        return {
            'prev_throttled_reads_cnt' : self.prev_throttled_reads_cnt,
            'curr_throttled_reads_cnt' : self.curr_throttled_reads_cnt,
            'prev_provision_reads_cnt' : self.prev_provision_reads_cnt,
            'curr_provision_reads_cnt' : self.curr_provision_reads_cnt,
            'prev_consuming_reads_cnt' : self.prev_consuming_reads_cnt,
            'curr_consuming_reads_cnt' : self.curr_consuming_reads_cnt,

            'prev_throttled_write_cnt' : self.prev_throttled_write_cnt,
            'curr_throttled_write_cnt' : self.curr_throttled_write_cnt,
            'prev_provision_write_cnt' : self.prev_provision_write_cnt,
            'curr_provision_write_cnt' : self.curr_provision_write_cnt,
            'prev_consuming_write_cnt' : self.prev_consuming_write_cnt,
            'curr_consuming_write_cnt' : self.curr_consuming_write_cnt,

            'sampling_window_in_sec' : self.sampling_window_in_sec }

    def log_current_read_throughput_stats(self,
                                          curr_provision_reads_cnt,
                                          curr_consuming_reads_cnt,
                                          curr_throttled_reads_cnt) :
        self.prev_throttled_reads_cnt = self.curr_throttled_reads_cnt
        self.prev_provision_reads_cnt = self.curr_provision_reads_cnt
        self.prev_consuming_reads_cnt = self.curr_consuming_reads_cnt

        self.curr_throttled_reads_cnt = curr_throttled_reads_cnt
        self.curr_provision_reads_cnt = curr_provision_reads_cnt
        self.curr_consuming_reads_cnt = curr_consuming_reads_cnt

    def log_current_write_throughput_stats(self,
                                           curr_provision_write_cnt,
                                           curr_consuming_write_cnt,
                                           curr_throttled_write_cnt) :
        self.prev_throttled_write_cnt = self.curr_throttled_write_cnt
        self.prev_provision_write_cnt = self.curr_provision_write_cnt
        self.prev_consuming_write_cnt = self.curr_consuming_write_cnt


        self.curr_throttled_write_cnt = curr_throttled_write_cnt
        self.curr_provision_write_cnt = curr_provision_write_cnt
        self.curr_consuming_write_cnt = curr_consuming_write_cnt

    def get_delta_for_throttled_reads_over_total_requests(self):
        return (
            (float(self.curr_throttled_reads_cnt) - float(self.prev_throttled_reads_cnt)) /
            (float(self.curr_throttled_reads_cnt) + float(self.prev_consuming_reads_cnt))
        )

    def get_delta_for_throttled_reads_over_provisioned_throughput():
        return (
            (float(self.curr_throttled_reads_cnt) - float(self.prev_throttled_reads_cnt)) /
            (float(self.curr_provision_reads_cnt))
        )

    def get_delta_for_throttled_write_over_total_requests(self):
        return (
            (float(self.curr_throttled_write_cnt) - float(self.prev_throttled_write_cnt)) /
            (float(self.curr_throttled_write_cnt) + float(self.prev_consuming_write_cnt))
        )

    def get_delta_for_throttled_write_over_provisioned_throughput(self):
        return (
            (float(self.curr_throttled_write_cnt) - float(self.prev_throttled_write_cnt)) /
            (float(self.curr_provision_write_cnt))
        )

    def get_sampling_window_in_sec(self):
        return self.sampling_window_in_sec

    def set_sampling_window_in_sec(self, sampling_window_in_sec):
        self.sampling_window_in_sec = sampling_window_in_sec
