class FeatureIndex:
    def __init__(self):
        self.index_counter = 0
        self.feature_name_to_index = {}
    def register(self, feature_name):
        self.feature_name_to_index[feature_name] = self.index_counter
        self.index_counter += 1
    def get_index(self, feature_name):
        return self.feature_name_to_index[feature_name]
    def drop(self, feature_name):
        new_dict = {}
        drop_index = 10000
        for key in self.feature_name_to_index:
            new_dict[key] = self.feature_name_to_index[key]
        for key in self.feature_name_to_index:
            if key == feature_name:
                new_dict.pop(key)
                self.index_counter -= 1
                drop_index = self.feature_name_to_index[key]
        for key in new_dict:    
            if new_dict[key] > drop_index:
                new_dict[key] -= 1
        self.feature_name_to_index = new_dict
