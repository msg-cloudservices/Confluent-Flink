from dataclasses import dataclass
from typing import Optional


@dataclass
class IdentityRecord:
    TransactionID: int
    id_01: Optional[float]
    id_02: Optional[float]
    id_03: Optional[float]
    id_04: Optional[float]
    id_05: Optional[float]
    id_06: Optional[float]
    id_07: Optional[float]
    id_08: Optional[float]
    id_09: Optional[float]
    id_10: Optional[float]
    id_11: Optional[float]
    id_12: Optional[str]
    id_13: Optional[float]
    id_14: Optional[float]
    id_15: Optional[str]
    id_16: Optional[str]
    id_17: Optional[float]
    id_18: Optional[float]
    id_19: Optional[float]
    id_20: Optional[float]
    id_21: Optional[float]
    id_22: Optional[float]
    id_23: Optional[str]
    id_24: Optional[float]
    id_25: Optional[float]
    id_26: Optional[float]
    id_27: Optional[str]
    id_28: Optional[str]
    id_29: Optional[str]
    id_30: Optional[str]
    id_31: Optional[str]
    id_32: Optional[float]
    id_33: Optional[str]
    id_34: Optional[str]
    id_35: Optional[str]
    id_36: Optional[str]
    id_37: Optional[str]
    id_38: Optional[str]
    DeviceType: Optional[str]
    DeviceInfo: Optional[str]
    TransactionDT: float


    def to_dict(self) -> dict:
        return {
            "TransactionID": self.TransactionID,
            "id_01": self.id_01,
            "id_02": self.id_02,
            "id_03": self.id_03,
            "id_04": self.id_04,
            "id_05": self.id_05,
            "id_06": self.id_06,
            "id_07": self.id_07,
            "id_08": self.id_08,
            "id_09": self.id_09,
            "id_10": self.id_10,
            "id_11": self.id_11,
            "id_12": self.id_12,
            "id_13": self.id_13,
            "id_14": self.id_14,
            "id_15": self.id_15,
            "id_16": self.id_16,
            "id_17": self.id_17,
            "id_18": self.id_18,
            "id_19": self.id_19,
            "id_20": self.id_20,
            "id_21": self.id_21,
            "id_22": self.id_22,
            "id_23": self.id_23,
            "id_24": self.id_24,
            "id_25": self.id_25,
            "id_26": self.id_26,
            "id_27": self.id_27,
            "id_28": self.id_28,
            "id_29": self.id_29,
            "id_30": self.id_30,
            "id_31": self.id_31,
            "id_32": self.id_32,
            "id_33": self.id_33,
            "id_34": self.id_34,
            "id_35": self.id_35,
            "id_36": self.id_36,
            "id_37": self.id_37,
            "id_38": self.id_38,
            "DeviceType": self.DeviceType,
            "DeviceInfo": self.DeviceInfo,
            "TransactionDT": self.TransactionDT,
        }
