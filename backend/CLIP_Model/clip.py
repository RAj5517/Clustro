# clip_backend.py
import torch
import torch.nn.functional as F
import open_clip

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"


class CLIPBackend:
    def __init__(self, model_name: str = "ViT-B-32", pretrained: str = "openai"):
        self.device = DEVICE
        self.model, _, self.preprocess = open_clip.create_model_and_transforms(
            model_name,
            pretrained=pretrained,
            device=self.device,
        )
        self.model.eval()
        self.tokenizer = open_clip.get_tokenizer(model_name)

    @torch.no_grad()
    def encode_image_tensor(self, image_tensor: torch.Tensor) -> torch.Tensor:
        """
        image_tensor: (1, 3, H, W) already preprocessed
        returns: (dim,)
        """
        image_tensor = image_tensor.to(self.device)
        feats = self.model.encode_image(image_tensor)
        feats = F.normalize(feats, dim=-1)
        return feats[0].cpu()

    @torch.no_grad()
    def encode_text(self, text: str) -> torch.Tensor:
        tokens = self.tokenizer([text]).to(self.device)
        feats = self.model.encode_text(tokens)
        feats = F.normalize(feats, dim=-1)
        return feats[0].cpu()
