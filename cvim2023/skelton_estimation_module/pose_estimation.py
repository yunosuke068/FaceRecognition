import mediapipe as mp
from sklearn.metrics.pairwise import cosine_similarity, cosine_distances
import cv2

class PoseEstimation:
    def __init__(self):
        # mediapipeの変数
        self.mp_holistic = mp.solutions.holistic
        self.holistic = self.mp_holistic.Holistic(static_image_mode=True, model_complexity=2)
        # self.holistic = self.mp_holistic.Holistic(min_detection_confidence=0.5,min_tracking_confidence=0.5)
        self.mp_drawing = mp.solutions.drawing_utils
        self.drawing_spec = self.mp_drawing.DrawingSpec(thickness=1, circle_radius=1)
        self.mp_drawing_styles = mp.solutions.drawing_styles
    
    def set_image(self, img):
        self.image_width = img.shape[1]
        self.image_height = img.shape[0]
        self.result = self.holistic.process(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))

    def get_landmarks(self,key): # key = pose || face || right_hand || left_hand
        if key == 'pose':
            if self.result.pose_landmarks: return self.landmark_list(self.result.pose_landmarks)
        elif key == 'face':
            if self.result.face_landmarks: return  self.landmark_list(self.result.face_landmarks)
        elif key == 'right_hand':
            if self.result.right_hand_landmarks: return self.landmark_list(self.result.right_hand_landmarks)
        elif key == 'left_hand':
            if self.result.left_hand_landmarks:return self.landmark_list(self.result.left_hand_landmarks)

        return [[]]

    def landmark_list(self,landmarks):
        return  [[row.x,row.y,row.z,row.visibility] for row in landmarks.landmark]